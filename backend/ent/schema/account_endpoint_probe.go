package schema

import (
	"entgo.io/ent"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// AccountEndpointProbe stores real probe evidence per (account, connection, provider, protocol, endpoint, credential+config version).
// Validity is 24 hours (expires_at = probed_at + 24h). Append-only evidence.
type AccountEndpointProbe struct {
	ent.Schema
}

func (AccountEndpointProbe) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "account_endpoint_probes"},
	}
}

func (AccountEndpointProbe) Fields() []ent.Field {
	return []ent.Field{
		field.Int64("account_id").
			Comment("FK to accounts"),
		field.Int64("connection_id").
			Comment("FK to upstream_connections"),
		field.String("provider").
			MaxLen(32).
			NotEmpty().
			Comment("governed provider"),
		field.String("protocol").
			MaxLen(32).
			NotEmpty().
			Comment("anthropic | openai | gemini"),
		field.String("normalized_endpoint_path").
			NotEmpty().
			SchemaType(map[string]string{dialect.Postgres: "text"}),
		field.Int64("credential_version").
			Comment(">0"),
		field.Int64("config_version").
			Comment(">0"),
		field.String("status").
			MaxLen(32).
			NotEmpty().
			Comment("success or failed"),
		field.Time("probed_at").
			SchemaType(map[string]string{dialect.Postgres: "timestamptz"}),
		field.Time("expires_at").
			SchemaType(map[string]string{dialect.Postgres: "timestamptz"}),
		field.String("evidence_ref").
			Optional().
			Nillable().
			SchemaType(map[string]string{dialect.Postgres: "text"}),
		field.String("request_fingerprint").
			Optional().
			Nillable().
			SchemaType(map[string]string{dialect.Postgres: "text"}),
		field.JSON("response_summary", map[string]any{}).
			Default(func() map[string]any { return map[string]any{} }).
			SchemaType(map[string]string{dialect.Postgres: "jsonb"}),
	}
}

func (AccountEndpointProbe) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("account", Account.Type).
			Ref("endpoint_probes").
			Field("account_id").
			Unique().
			Required(),
		edge.From("connection", UpstreamConnection.Type).
			Ref("connection_probes").
			Field("connection_id").
			Unique().
			Required(),
	}
}

func (AccountEndpointProbe) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("account_id", "probed_at"),
		index.Fields("connection_id", "probed_at"),
		index.Fields("expires_at"),
		index.Fields("provider", "protocol"),
		// Uniqueness is enforced by trigger for exact text, but add index to aid queries.
		index.Fields("account_id", "connection_id", "provider", "protocol", "credential_version", "config_version"),
	}
}
