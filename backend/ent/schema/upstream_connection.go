package schema

import (
	"github.com/Wei-Shaw/sub2api/ent/schema/mixins"

	"entgo.io/ent"
	"entgo.io/ent/dialect"
	"entgo.io/ent/dialect/entsql"
	"entgo.io/ent/schema"
	"entgo.io/ent/schema/edge"
	"entgo.io/ent/schema/field"
	"entgo.io/ent/schema/index"
)

// UpstreamConnection holds reusable credentials/network identity separate from provider accounts.
// Aggregator connection provider is null; first-party provider is concrete and immutable.
// Phase 4: credentials are encrypted via SecretEncryptor.
type UpstreamConnection struct {
	ent.Schema
}

func (UpstreamConnection) Annotations() []schema.Annotation {
	return []schema.Annotation{
		entsql.Annotation{Table: "upstream_connections"},
	}
}

func (UpstreamConnection) Mixin() []ent.Mixin {
	return []ent.Mixin{
		mixins.TimeMixin{},
		mixins.SoftDeleteMixin{},
	}
}

func (UpstreamConnection) Fields() []ent.Field {
	return []ent.Field{
		field.String("kind").
			MaxLen(32).
			NotEmpty().
			Comment("first_party or aggregator"),
		field.String("provider").
			MaxLen(32).
			Optional().
			Nillable().
			Comment("governed provider or null for aggregator"),
		field.String("base_url").
			NotEmpty().
			SchemaType(map[string]string{dialect.Postgres: "text"}),
		field.String("encrypted_credential").
			NotEmpty().
			SchemaType(map[string]string{dialect.Postgres: "text"}),
		field.Int64("credential_version").
			Default(1).
			Comment("optimistic version, >0"),
		field.Int64("proxy_id").
			Optional().
			Nillable().
			Comment("optional proxy id"),
		field.String("status").
			MaxLen(32).
			Default("active"),
		field.String("evidence_ref").
			Optional().
			Nillable().
			SchemaType(map[string]string{dialect.Postgres: "text"}),
	}
}

func (UpstreamConnection) Edges() []ent.Edge {
	return []ent.Edge{
		edge.From("accounts", Account.Type).
			Ref("connection"),
		edge.To("connection_probes", AccountEndpointProbe.Type),
		edge.To("proxy", Proxy.Type).
			Field("proxy_id").
			Unique(),
	}
}

func (UpstreamConnection) Indexes() []ent.Index {
	return []ent.Index{
		index.Fields("kind", "provider"),
		index.Fields("proxy_id"),
		index.Fields("status"),
		index.Fields("deleted_at"),
	}
}
