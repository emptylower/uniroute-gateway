-- Vendor platform completion: deepseek/glm/kimi/qwen/longcat/bytedance/minimax
-- become first-class governed providers. These vendors ride the OpenAI wire
-- format (chat/completions + /v1/models); protocol CHECKs stay unchanged.
-- Widen every provider/platform CHECK that was locked to the original set.

-- usage_logs.governance_target_platform (original 5 incl. antigravity)
ALTER TABLE usage_logs DROP CONSTRAINT chk_usage_logs_governance_target_platform;
ALTER TABLE usage_logs ADD CONSTRAINT chk_usage_logs_governance_target_platform
    CHECK (governance_target_platform IS NULL OR governance_target_platform IN
        ('anthropic','openai','gemini','antigravity','grok',
         'deepseek','glm','kimi','qwen','longcat','bytedance','minimax'));

-- channel monitors (monitoring providers)
ALTER TABLE channel_monitors DROP CONSTRAINT channel_monitors_provider_check;
ALTER TABLE channel_monitors ADD CONSTRAINT channel_monitors_provider_check
    CHECK (provider IN ('openai','anthropic','gemini','grok',
        'deepseek','glm','kimi','qwen','longcat','bytedance','minimax'));

ALTER TABLE channel_monitor_request_templates DROP CONSTRAINT channel_monitor_request_templates_provider_check;
ALTER TABLE channel_monitor_request_templates ADD CONSTRAINT channel_monitor_request_templates_provider_check
    CHECK (provider IN ('openai','anthropic','gemini','grok',
        'deepseek','glm','kimi','qwen','longcat','bytedance','minimax'));

-- per-user per-platform quotas
ALTER TABLE user_platform_quotas DROP CONSTRAINT user_platform_quotas_platform_check;
ALTER TABLE user_platform_quotas ADD CONSTRAINT user_platform_quotas_platform_check
    CHECK (platform IN ('anthropic','openai','gemini','antigravity','grok',
        'deepseek','glm','kimi','qwen','longcat','bytedance','minimax'));

-- composite routing targets
ALTER TABLE composite_model_routes DROP CONSTRAINT composite_model_routes_target_platform_check;
ALTER TABLE composite_model_routes ADD CONSTRAINT composite_model_routes_target_platform_check
    CHECK (target_platform IN ('anthropic','openai','gemini','antigravity','grok',
        'deepseek','glm','kimi','qwen','longcat','bytedance','minimax'));

-- governance: model registry
ALTER TABLE model_registry DROP CONSTRAINT chk_model_registry_provider;
ALTER TABLE model_registry ADD CONSTRAINT chk_model_registry_provider
    CHECK (provider IN ('anthropic','openai','gemini','grok',
        'deepseek','glm','kimi','qwen','longcat','bytedance','minimax'));

-- governance: shadow decisions
ALTER TABLE model_shadow_decisions DROP CONSTRAINT chk_model_shadow_decisions_provider;
ALTER TABLE model_shadow_decisions ADD CONSTRAINT chk_model_shadow_decisions_provider
    CHECK (provider IS NULL OR provider IN ('anthropic','openai','gemini','grok',
        'deepseek','glm','kimi','qwen','longcat','bytedance','minimax'));

-- governance: upstream connections
ALTER TABLE upstream_connections DROP CONSTRAINT chk_upstream_connections_provider;
ALTER TABLE upstream_connections ADD CONSTRAINT chk_upstream_connections_provider
    CHECK (provider IS NULL OR provider IN ('anthropic','openai','gemini','grok',
        'deepseek','glm','kimi','qwen','longcat','bytedance','minimax'));

ALTER TABLE upstream_connections DROP CONSTRAINT chk_upstream_connections_kind_provider;
ALTER TABLE upstream_connections ADD CONSTRAINT chk_upstream_connections_kind_provider
    CHECK ((kind = 'aggregator' AND provider IS NULL) OR
           (kind = 'first_party' AND provider IN ('anthropic','openai','gemini','grok',
               'deepseek','glm','kimi','qwen','longcat','bytedance','minimax')));

-- governance: endpoint probes
ALTER TABLE account_endpoint_probes DROP CONSTRAINT chk_account_endpoint_probes_provider;
ALTER TABLE account_endpoint_probes ADD CONSTRAINT chk_account_endpoint_probes_provider
    CHECK (provider IN ('anthropic','openai','gemini','grok',
        'deepseek','glm','kimi','qwen','longcat','bytedance','minimax'));

-- governance: aggregator reuse requests
ALTER TABLE aggregator_reuse_requests DROP CONSTRAINT chk_aggregator_reuse_requests_provider;
ALTER TABLE aggregator_reuse_requests ADD CONSTRAINT chk_aggregator_reuse_requests_provider
    CHECK (provider IN ('anthropic','openai','gemini','grok',
        'deepseek','glm','kimi','qwen','longcat','bytedance','minimax'));
