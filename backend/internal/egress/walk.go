package egress

import (
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

// Annotation is what the classification in spec §2.0 hangs on.
type Annotation string

const (
	AnnotationPort                      Annotation = "port"                         // the two decorated ports and the two WS dialer construction sites
	AnnotationNonBillable               Annotation = "non-billable"                 // carries the explicit mark
	AnnotationBillable                  Annotation = "billable"                     // a write through a decorated port that carries a handle
	AnnotationBillableOutsideThisWallet Annotation = "billable-outside-this-wallet" // Batch Image (spec §2.5)
)

// EgressEntry is one pinned egress construct: a file importing an egress-capable
// package that also constructs a client/dialer, or a function returning
// *http.Client / http.RoundTripper.
type EgressEntry struct {
	File       string     `json:"file"`   // repo-relative, forward slashes
	Kind       string     `json:"kind"`   // "import" | "factory" | "construct"
	Detail     string     `json:"detail"` // the import path, the function name, or the construct
	Annotation Annotation `json:"annotation"`
	Note       string     `json:"note,omitempty"`
}

func (e EgressEntry) Key() string { return e.File + "|" + e.Kind + "|" + e.Detail }

// WriteSite is one httpUpstream.Do / DoWithTLS call, keyed by file + enclosing
// function + ordinal within that function — stable across line drift.
type WriteSite struct {
	File       string     `json:"file"`
	Func       string     `json:"func"`
	Ordinal    int        `json:"ordinal"`
	Method     string     `json:"method"` // Do | DoWithTLS
	Annotation Annotation `json:"annotation"`
	Note       string     `json:"note,omitempty"`

	// Derived by the walk, never pinned:
	EnclosingFuncCarriesNonBillableMark bool `json:"-"`
	EnclosingFuncShortCircuitsRefusal   bool `json:"-"`
	Line                                int  `json:"-"`
}

func (s WriteSite) Key() string { return s.File + "|" + s.Func + "|" + strconv.Itoa(s.Ordinal) }

// egressImports: a file importing any of these is an egress candidate at IMPORT
// granularity (spec §2.0 — gRPC, other WebSocket libraries and vendor SDKs are one
// import away). Because "net" is included with prefix matching, net/http is matched
// at import granularity (so any file importing net/http has its egress walked),
// while http.Client / http.Transport and write sites are the specific constructs
// inspected within those files.
var egressImports = []string{
	"net", "crypto/tls",
	"github.com/coder/websocket", "nhooyr.io/websocket", "github.com/gorilla/websocket", "golang.org/x/net/websocket",
	"google.golang.org/grpc",
	"cloud.google.com/go", "google.golang.org/api", "github.com/aws/aws-sdk-go", "github.com/aws/aws-sdk-go-v2",
	"github.com/openai/openai-go", "github.com/anthropics/anthropic-sdk-go", "github.com/sashabaranov/go-openai",
}

// Walk parses every non-test .go file under root/internal and returns the egress
// entries and the upstream write sites it finds.
func Walk(root string) (entries []EgressEntry, sites []WriteSite, err error) {
	fset := token.NewFileSet()
	err = filepath.WalkDir(filepath.Join(root, "internal"), func(path string, d fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if d.IsDir() {
			// Go's toolchain ignores testdata; filepath.WalkDir does not — without this
			// the walker ingests its own testdata/fixture tree (round-1 finding).
			if name := d.Name(); name == "testdata" || name == "vendor" || name == "node_modules" {
				return filepath.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		rel, relErr := filepath.Rel(root, path)
		if relErr != nil {
			return relErr
		}
		rel = filepath.ToSlash(rel)
		f, perr := parser.ParseFile(fset, path, nil, parser.ParseComments)
		if perr != nil {
			return perr
		}
		// imports
		importsNetHTTP := false
		for _, imp := range f.Imports {
			p := strings.Trim(imp.Path.Value, `"`)
			if p == "net/http" {
				importsNetHTTP = true
			}
			for _, eg := range egressImports {
				if p == eg || strings.HasPrefix(p, eg+"/") {
					entries = append(entries, EgressEntry{File: rel, Kind: "import", Detail: p})
				}
			}
		}
		// factories, constructs and write sites
		for _, decl := range f.Decls {
			fn, ok := decl.(*ast.FuncDecl)
			if !ok {
				continue
			}
			if fn.Type.Results != nil {
				for _, r := range fn.Type.Results.List {
					if isHTTPClientOrRoundTripper(r.Type) {
						entries = append(entries, EgressEntry{File: rel, Kind: "factory", Detail: funcName(fn)})
					}
				}
			}
			if fn.Body == nil {
				continue
			}
			ordinal := 0
			marks, shortCircuits := false, false
			ast.Inspect(fn.Body, func(n ast.Node) bool {
				switch x := n.(type) {
				case *ast.CompositeLit:
					if importsNetHTTP && isSelectorType(x.Type, "http", "Client") {
						entries = append(entries, EgressEntry{File: rel, Kind: "construct", Detail: funcName(fn) + ": &http.Client{}"})
					}
				case *ast.SelectorExpr:
					if importsNetHTTP && isSelector(x, "http", "DefaultClient") {
						entries = append(entries, EgressEntry{File: rel, Kind: "construct", Detail: funcName(fn) + ": http.DefaultClient"})
					}
				case *ast.CallExpr:
					// Package-local calls in internal/service carry no selector:
					// AsAuthorizationRefused(err) / WithNonBillableUpstream(ctx, …).
					if id, ok := x.Fun.(*ast.Ident); ok {
						switch id.Name {
						case "WithNonBillableUpstream":
							marks = true
						case "AsAuthorizationRefused":
							shortCircuits = true
						}
						return true
					}
					sel, ok := x.Fun.(*ast.SelectorExpr)
					if !ok {
						return true
					}
					switch {
					case importsNetHTTP && isSelector(sel, "http", "Get"), importsNetHTTP && isSelector(sel, "http", "Post"),
						importsNetHTTP && isSelector(sel, "http", "Head"), importsNetHTTP && isSelector(sel, "http", "PostForm"):
						entries = append(entries, EgressEntry{File: rel, Kind: "construct", Detail: funcName(fn) + ": http." + sel.Sel.Name})
					case sel.Sel.Name == "RoundTrip" && importsNetHTTP:
						entries = append(entries, EgressEntry{File: rel, Kind: "construct", Detail: funcName(fn) + ": RoundTrip"})
					case (sel.Sel.Name == "Do" || sel.Sel.Name == "DoWithTLS") && receiverIsHTTPUpstream(sel.X):
						ordinal++
						sites = append(sites, WriteSite{File: rel, Func: funcName(fn), Ordinal: ordinal, Method: sel.Sel.Name, Line: fset.Position(x.Pos()).Line})
					case sel.Sel.Name == "WithNonBillableUpstream":
						marks = true
					case sel.Sel.Name == "AsAuthorizationRefused":
						shortCircuits = true
					case sel.Sel.Name == "Is" && isIdent(sel.X, "errors") && len(x.Args) == 2 && isSelectorOrIdentNamed(x.Args[1], "ErrAuthorizationRefused"):
						shortCircuits = true
					}
				}
				return true
			})
			for i := range sites {
				if sites[i].File == rel && sites[i].Func == funcName(fn) {
					sites[i].EnclosingFuncCarriesNonBillableMark = marks
					sites[i].EnclosingFuncShortCircuitsRefusal = shortCircuits
				}
			}
		}
		return nil
	})
	sort.Slice(entries, func(i, j int) bool { return entries[i].Key() < entries[j].Key() })
	sort.Slice(sites, func(i, j int) bool { return sites[i].Key() < sites[j].Key() })
	// Identical constructs (same file, kind and detail — e.g. two methods whose
	// bodies each call RoundTrip) collapse to one pin: a duplicate would make
	// the pin comparison double-count the same key.
	entries = dedupeEntries(entries)
	return entries, sites, err
}

func dedupeEntries(entries []EgressEntry) []EgressEntry {
	seen := make(map[string]struct{}, len(entries))
	out := entries[:0]
	for _, e := range entries {
		if _, ok := seen[e.Key()]; ok {
			continue
		}
		seen[e.Key()] = struct{}{}
		out = append(out, e)
	}
	return out
}

// isSelectorType reports whether t is pkg.name (used for composite literal types).
func isSelectorType(t ast.Expr, pkg, name string) bool {
	sel, ok := t.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	return isSelector(sel, pkg, name)
}

// isSelector reports whether expr is the selector pkg.name.
func isSelector(expr ast.Expr, pkg, name string) bool {
	sel, ok := expr.(*ast.SelectorExpr)
	if !ok {
		return false
	}
	return isIdent(sel.X, pkg) && sel.Sel.Name == name
}

// isIdent reports whether expr is the identifier name.
func isIdent(expr ast.Expr, name string) bool {
	id, ok := expr.(*ast.Ident)
	return ok && id.Name == name
}

// isSelectorOrIdentNamed reports whether expr is a selector or identifier whose
// base name is name (e.g. ErrAuthorizationRefused, pkg.ErrAuthorizationRefused).
func isSelectorOrIdentNamed(expr ast.Expr, name string) bool {
	switch x := expr.(type) {
	case *ast.Ident:
		return x.Name == name
	case *ast.SelectorExpr:
		return x.Sel.Name == name
	}
	return false
}

// isHTTPClientOrRoundTripper reports whether t is *http.Client or http.RoundTripper.
func isHTTPClientOrRoundTripper(t ast.Expr) bool {
	if star, ok := t.(*ast.StarExpr); ok {
		return isSelector(star.X, "http", "Client")
	}
	return isSelector(t, "http", "RoundTripper")
}

// funcName renders receiver type + "." + name for a function declaration.
func funcName(fn *ast.FuncDecl) string {
	name := fn.Name.Name
	if fn.Recv != nil && len(fn.Recv.List) > 0 {
		recv := fn.Recv.List[0].Type
		if star, ok := recv.(*ast.StarExpr); ok {
			recv = star.X
		}
		if id, ok := recv.(*ast.Ident); ok {
			return id.Name + "." + name
		}
	}
	return name
}

// receiverIsHTTPUpstream matches the receiver expression of every upstream write:
// `s.httpUpstream.Do`, `p.httpUpstream.Do`, `s.accountTestService.httpUpstream.Do`,
// or a bare `httpUpstream.Do` identifier.
func receiverIsHTTPUpstream(x ast.Expr) bool {
	if id, ok := x.(*ast.Ident); ok {
		return id.Name == "httpUpstream"
	}
	if sel, ok := x.(*ast.SelectorExpr); ok {
		return sel.Sel.Name == "httpUpstream"
	}
	return false
}
