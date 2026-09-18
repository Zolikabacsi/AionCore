use aionui_api_types::{DELEGATE_TOOLS_SCHEMA_VERSION, delegate_tool_descriptors};
use serde_json::{Value, json};

pub(crate) fn data() -> Value {
    let tools = delegate_tool_descriptors()
        .into_iter()
        .map(|tool| {
            json!({
                "name": tool.name,
                "cli_command": tool.cli_command,
                "description": tool.description,
                "when": tool.when,
                "input_summary": tool.input_summary,
                "stdin_json_schema": tool.input_schema,
            })
        })
        .collect::<Vec<_>>();
    json!({
        "schema_version": DELEGATE_TOOLS_SCHEMA_VERSION,
        "contract": "agent-facing-delegate-cli",
        "commands": {
            "capabilities": { "runtime_env_required": [] },
            "targets": { "runtime_env_required": ["AIONUI_BASE_URL", "AIONUI_USER_ID", "AIONUI_CONVERSATION_ID", "AIONUI_RUNTIME_TOKEN"] },
            "dispatch": { "runtime_env_required": ["AIONUI_BASE_URL", "AIONUI_USER_ID", "AIONUI_CONVERSATION_ID", "AIONUI_RUNTIME_TOKEN"] },
            "ask": { "runtime_env_required": ["AIONUI_BASE_URL", "AIONUI_USER_ID", "AIONUI_CONVERSATION_ID", "AIONUI_RUNTIME_TOKEN"] }
        },
        "output_envelope": {
            "success": "boolean",
            "data": "object when success=true",
            "error": "object when success=false",
            "meta": { "schema_version": DELEGATE_TOOLS_SCHEMA_VERSION }
        },
        "delivery_status": {
            "delivered": "turn claim taken, message persisted, prompt dispatched (or merged into the target's running turn)",
            "queued": "target not ready yet; queued in memory and retried until it frees up",
            "created_and_delivered": "no prior conversation existed for the target; the runtime created one and delivered the message"
        },
        "tools": tools,
        "errors": [
            "target_not_found",
            "ambiguous_target",
            "delegation_disabled_for_target",
            "delegation_disabled_for_sender",
            "target_is_self",
            "reply_target_not_owned",
            "cycle_detected",
            "depth_exceeded",
            "rate_limited",
            "queue_full",
            "feature_disabled",
            "runtime_auth_failed",
            "schema_validation_failed",
            "transport_unavailable",
            "sync_timeout"
        ]
    })
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn capabilities_lists_every_registry_tool_and_its_cli_path() {
        let data = data();
        let tools = data["tools"].as_array().unwrap();
        assert_eq!(tools.len(), delegate_tool_descriptors().len());
        for descriptor in delegate_tool_descriptors() {
            let entry = tools
                .iter()
                .find(|tool| tool["name"] == serde_json::json!(descriptor.name))
                .unwrap_or_else(|| panic!("{} missing from capabilities", descriptor.name));
            assert_eq!(
                entry["cli_command"],
                serde_json::to_value(&descriptor.cli_command).unwrap()
            );
            assert!(entry["stdin_json_schema"].is_object(), "{}", descriptor.name);
        }
    }

    #[test]
    fn capabilities_declares_that_it_needs_no_runtime_env() {
        let data = data();
        assert_eq!(
            data["commands"]["capabilities"]["runtime_env_required"],
            serde_json::json!([])
        );
    }

    #[test]
    fn every_error_code_is_documented() {
        let data = data();
        let documented: Vec<&str> = data["errors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|value| value.as_str().unwrap())
            .collect();
        for expected in [
            "target_not_found",
            "ambiguous_target",
            "cycle_detected",
            "depth_exceeded",
            "rate_limited",
            "queue_full",
            "feature_disabled",
            "runtime_auth_failed",
            "schema_validation_failed",
            "transport_unavailable",
            "sync_timeout",
        ] {
            assert!(documented.contains(&expected), "{expected} is undocumented");
        }
    }
}
