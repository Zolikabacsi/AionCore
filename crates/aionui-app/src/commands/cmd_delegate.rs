//! `aioncore delegate` — the agent-facing cross-agent delegation CLI.
//!
//! Mirrors `cmd_session.rs` shape. Two surfaces:
//!
//! - HTTP bridge to `/api/runtime/delegate/{dispatch,ask,targets}`.
//! - Stdin payload validation against the descriptor registry before the
//!   round trip, so a typo comes back as `schema_validation_failed` locally
//!   rather than as a confusing server-side rejection.

use std::ffi::OsString;
use std::io::{self, Read, Write};
use std::process::ExitCode;

use aionui_api_types::{DelegateCliEnvelope, DelegateToolErrorCode, DelegateToolErrorPayload, DelegateToolName};
use serde_json::{Value, json};

use crate::cli::{DelegateArgs, DelegateCommand};
use crate::commands::delegate_capabilities;

const ENV_BASE_URL: &str = "AIONUI_BASE_URL";
const ENV_USER_ID: &str = "AIONUI_USER_ID";
const ENV_CONVERSATION_ID: &str = "AIONUI_CONVERSATION_ID";
const ENV_RUNTIME_TOKEN: &str = "AIONUI_RUNTIME_TOKEN";

pub(crate) async fn run_delegate(args: DelegateArgs) -> ExitCode {
    match run_delegate_inner(args).await {
        Ok(()) => ExitCode::SUCCESS,
        Err(code) => code,
    }
}

async fn run_delegate_inner(args: DelegateArgs) -> Result<(), ExitCode> {
    match args.command {
        DelegateCommand::Capabilities => print_json(&DelegateCliEnvelope::success(
            delegate_capabilities::data(),
            Some("delegate capabilities".to_owned()),
        )),
        DelegateCommand::Targets => targets().await,
        DelegateCommand::Dispatch => dispatch().await,
        DelegateCommand::Ask => ask().await,
        DelegateCommand::Unknown(path) => Err(unknown_command("delegate", path, "unknown delegate command")),
    }
}

async fn targets() -> Result<(), ExitCode> {
    let command = "delegate targets";
    let env = runtime_env(command)?;
    let arguments = read_stdin_json_object(command, DelegateToolName::DelegateTargets)?;
    let query = query_string(&arguments);
    let url = format!(
        "{}/api/runtime/delegate/targets{query}",
        env.base_url.trim_end_matches('/')
    );
    let response = reqwest::Client::new()
        .get(url)
        .headers(env.headers(command)?)
        .send()
        .await
        .map_err(|error| runtime_error(command, "DELEGATE_CLI_HTTP_BRIDGE_FAILED", error.to_string()))?;
    print_response(command, response).await
}

async fn dispatch() -> Result<(), ExitCode> {
    let command = "delegate dispatch";
    let env = runtime_env(command)?;
    let body = read_stdin_json_object(command, DelegateToolName::DelegateDispatch)?;
    let url = format!(
        "{}/api/runtime/delegate/dispatch",
        env.base_url.trim_end_matches('/')
    );
    let response = reqwest::Client::new()
        .post(url)
        .headers(env.headers(command)?)
        .json(&body)
        .send()
        .await
        .map_err(|error| runtime_error(command, "DELEGATE_CLI_HTTP_BRIDGE_FAILED", error.to_string()))?;
    print_response(command, response).await
}

async fn ask() -> Result<(), ExitCode> {
    let command = "delegate ask";
    let env = runtime_env(command)?;
    let body = read_stdin_json_object(command, DelegateToolName::DelegateAsk)?;
    let url = format!(
        "{}/api/runtime/delegate/ask",
        env.base_url.trim_end_matches('/')
    );
    let response = reqwest::Client::new()
        .post(url)
        .headers(env.headers(command)?)
        .json(&body)
        .send()
        .await
        .map_err(|error| runtime_error(command, "DELEGATE_CLI_HTTP_BRIDGE_FAILED", error.to_string()))?;
    print_response(command, response).await
}

fn query_string(arguments: &Value) -> String {
    let Some(object) = arguments.as_object() else {
        return String::new();
    };
    let pairs: Vec<String> = object
        .iter()
        .filter_map(|(key, value)| {
            let rendered = match value {
                Value::String(text) => text.clone(),
                Value::Number(number) => number.to_string(),
                Value::Bool(flag) => flag.to_string(),
                _ => return None,
            };
            Some(format!("{}={}", urlencode(key), urlencode(&rendered)))
        })
        .collect();
    if pairs.is_empty() {
        String::new()
    } else {
        format!("?{}", pairs.join("&"))
    }
}

fn urlencode(value: &str) -> String {
    let mut encoded = String::with_capacity(value.len());
    for byte in value.bytes() {
        match byte {
            b'A'..=b'Z' | b'a'..=b'z' | b'0'..=b'9' | b'-' | b'_' | b'.' | b'~' => encoded.push(byte as char),
            _ => encoded.push_str(&format!("%{byte:02X}")),
        }
    }
    encoded
}

struct RuntimeEnv {
    base_url: String,
    user_id: String,
    conversation_id: String,
    runtime_token: String,
}

impl RuntimeEnv {
    fn headers(&self, command: &str) -> Result<reqwest::header::HeaderMap, ExitCode> {
        let mut headers = reqwest::header::HeaderMap::new();
        for (name, value) in [
            ("x-aionui-user-id", &self.user_id),
            ("x-aionui-conversation-id", &self.conversation_id),
            ("x-aionui-runtime-token", &self.runtime_token),
        ] {
            let parsed = value.parse().map_err(|_| {
                runtime_error(
                    command,
                    "DELEGATE_CLI_HEADER_INVALID",
                    format!("environment variable for {name} is not a valid header value"),
                )
            })?;
            headers.insert(name, parsed);
        }
        Ok(headers)
    }
}

fn runtime_env(command: &str) -> Result<RuntimeEnv, ExitCode> {
    Ok(RuntimeEnv {
        base_url: required_env(command, ENV_BASE_URL)?,
        user_id: required_env(command, ENV_USER_ID)?,
        conversation_id: required_env(command, ENV_CONVERSATION_ID)?,
        runtime_token: required_env(command, ENV_RUNTIME_TOKEN)?,
    })
}

fn required_env(command: &str, name: &'static str) -> Result<String, ExitCode> {
    std::env::var(name).map_err(|_| {
        print_failure(
            command,
            "DELEGATE_CLI_ENV_MISSING",
            DelegateToolErrorPayload::new(
                DelegateToolErrorCode::TransportUnavailable,
                format!("missing required environment variable: {name}"),
            ),
        )
    })
}

fn read_stdin_json_object(command: &str, tool: DelegateToolName) -> Result<Value, ExitCode> {
    let mut input = String::new();
    io::stdin().read_to_string(&mut input).map_err(|error| {
        print_failure(
            command,
            "DELEGATE_CLI_STDIN_READ_FAILED",
            DelegateToolErrorPayload::new(DelegateToolErrorCode::SchemaValidationFailed, error.to_string()),
        )
    })?;
    let value = if input.trim().is_empty() {
        json!({})
    } else {
        serde_json::from_str(&input).map_err(|error| {
            print_failure(
                command,
                "DELEGATE_CLI_STDIN_JSON_INVALID",
                DelegateToolErrorPayload::new(DelegateToolErrorCode::SchemaValidationFailed, error.to_string()),
            )
        })?
    };
    validate_against_descriptor(command, tool, value)
}

fn validate_against_descriptor(command: &str, tool: DelegateToolName, value: Value) -> Result<Value, ExitCode> {
    let Some(object) = value.as_object() else {
        return Err(print_failure(
            command,
            "DELEGATE_CLI_SCHEMA_VALIDATION_FAILED",
            DelegateToolErrorPayload::new(DelegateToolErrorCode::SchemaValidationFailed, "stdin JSON must be an object"),
        ));
    };
    let descriptor = aionui_api_types::delegate_tool_descriptor(tool.as_str()).expect("descriptor for canonical tool");
    let properties = descriptor.input_schema["properties"]
        .as_object()
        .cloned()
        .unwrap_or_default();
    for key in object.keys() {
        if !properties.contains_key(key) {
            return Err(print_failure(
                command,
                "DELEGATE_CLI_SCHEMA_VALIDATION_FAILED",
                DelegateToolErrorPayload::new(
                    DelegateToolErrorCode::SchemaValidationFailed,
                    format!("unknown stdin field: {key}"),
                )
                .with_details(json!({ "expected_schema": descriptor.input_schema })),
            ));
        }
    }
    if let Some(required) = descriptor.input_schema["required"].as_array() {
        for key in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(key) {
                return Err(print_failure(
                    command,
                    "DELEGATE_CLI_SCHEMA_VALIDATION_FAILED",
                    DelegateToolErrorPayload::new(
                        DelegateToolErrorCode::SchemaValidationFailed,
                        format!("missing required stdin field: {key}"),
                    )
                    .with_details(json!({ "expected_schema": descriptor.input_schema })),
                ));
            }
        }
    }
    Ok(value)
}

async fn print_response(command: &str, response: reqwest::Response) -> Result<(), ExitCode> {
    let status = response.status();
    let text = response
        .text()
        .await
        .map_err(|error| runtime_error(command, "DELEGATE_CLI_HTTP_RESPONSE_FAILED", error.to_string()))?;
    if !status.is_success() {
        eprintln!(
            "DELEGATE_CLI_HTTP_STATUS_ERROR command={command} status={status}: runtime bridge returned non-success status"
        );
        println!("{text}");
        return Err(ExitCode::from(3));
    }
    println!("{text}");
    Ok(())
}

fn runtime_error(command: &str, code: &'static str, message: String) -> ExitCode {
    print_failure(
        command,
        code,
        DelegateToolErrorPayload::new(DelegateToolErrorCode::TransportUnavailable, message),
    )
}

fn unknown_command(prefix: &str, path: Vec<OsString>, message: &'static str) -> ExitCode {
    let suffix = path
        .into_iter()
        .map(|part| part.to_string_lossy().into_owned())
        .collect::<Vec<_>>()
        .join(" ");
    let command = if suffix.is_empty() {
        prefix.to_owned()
    } else {
        format!("{prefix} {suffix}")
    };
    print_failure(
        &command,
        "DELEGATE_CLI_UNKNOWN_COMMAND",
        DelegateToolErrorPayload::new(DelegateToolErrorCode::SchemaValidationFailed, message),
    )
}

fn print_failure(command: &str, stderr_code: &'static str, error: DelegateToolErrorPayload) -> ExitCode {
    eprintln!("{stderr_code} command={command}: {}", error.message);
    let _ = print_json(&DelegateCliEnvelope::<Value>::failure(error, Some(command.to_owned())));
    ExitCode::from(2)
}

fn print_json<T: serde::Serialize>(value: &T) -> Result<(), ExitCode> {
    let rendered = serde_json::to_string_pretty(value).map_err(|_| ExitCode::from(1))?;
    let mut stdout = io::stdout();
    stdout
        .write_all(rendered.as_bytes())
        .and_then(|_| stdout.write_all(b"\n"))
        .map_err(|_| ExitCode::from(1))
}

#[cfg(test)]
mod tests {
    use aionui_api_types::tool_name_for_delegate_cli_path;

    use super::*;

    fn env(user_id: &str, conversation_id: &str, runtime_token: &str) -> RuntimeEnv {
        RuntimeEnv {
            base_url: "http://127.0.0.1:1".to_owned(),
            user_id: user_id.to_owned(),
            conversation_id: conversation_id.to_owned(),
            runtime_token: runtime_token.to_owned(),
        }
    }

    #[test]
    fn well_formed_runtime_env_produces_all_three_headers() {
        let headers = env("user_1", "conv_1", "tok_1")
            .headers("delegate dispatch")
            .expect("ordinary ids and tokens are valid header values");
        assert_eq!(headers.get("x-aionui-user-id").unwrap(), "user_1");
        assert_eq!(headers.get("x-aionui-conversation-id").unwrap(), "conv_1");
        assert_eq!(headers.get("x-aionui-runtime-token").unwrap(), "tok_1");
    }

    #[test]
    fn a_malformed_env_value_yields_an_exit_code_instead_of_panicking() {
        for broken in ["bad\nvalue", "bad\rvalue", "bad\0value"] {
            assert!(env(broken, "conv_1", "tok_1").headers("delegate dispatch").is_err());
            assert!(env("user_1", broken, "tok_1").headers("delegate dispatch").is_err());
            assert!(env("user_1", "conv_1", broken).headers("delegate dispatch").is_err());
        }
    }

    #[test]
    fn query_string_rendering_is_sane() {
        assert_eq!(query_string(&json!({})), "");
        assert_eq!(query_string(&json!({ "q": "cmo" })), "?q=cmo");
        assert_eq!(query_string(&json!({ "limit": 20 })), "?limit=20");
        assert!(query_string(&json!({ "q": ["a"] })).is_empty());
    }

    #[test]
    fn dispatch_requires_to_and_message() {
        let missing_message = validate_against_descriptor(
            "delegate dispatch",
            DelegateToolName::DelegateDispatch,
            json!({ "to": "CMO" }),
        );
        assert!(missing_message.is_err());
        let ok = validate_against_descriptor(
            "delegate dispatch",
            DelegateToolName::DelegateDispatch,
            json!({ "to": "CMO", "message": "audit the sitemap" }),
        );
        assert!(ok.is_ok());
    }

    #[test]
    fn unknown_stdin_field_is_rejected() {
        let r = validate_against_descriptor(
            "delegate dispatch",
            DelegateToolName::DelegateDispatch,
            json!({ "to": "CMO", "message": "hi", "unknown_field": "x" }),
        );
        assert!(r.is_err());
    }

    #[test]
    fn the_cli_paths_the_registry_advertises_resolve_to_tool_names() {
        assert_eq!(
            tool_name_for_delegate_cli_path(&["dispatch".to_owned()]),
            Some(DelegateToolName::DelegateDispatch)
        );
        assert_eq!(
            tool_name_for_delegate_cli_path(&["ask".to_owned()]),
            Some(DelegateToolName::DelegateAsk)
        );
        assert_eq!(
            tool_name_for_delegate_cli_path(&["targets".to_owned()]),
            Some(DelegateToolName::DelegateTargets)
        );
    }
}
