//! Email/set transport for deletion-capable JSON patches.
//!
//! jmap-client 0.4.1's Email patch map is bool-valued and cannot represent
//! null. Keep the raw JSON boundary here while retaining its response/error
//! types and the connected client's authentication, endpoint and timeout.

use anyhow::{Context, Result};
use jmap_client::client::Client;
use jmap_client::core::response::{EmailSetResponse, Response, SingleMethodResponse};
use serde_json::{Value, json};

pub(super) fn http_client(client: &Client) -> Result<reqwest::Client> {
    reqwest::Client::builder()
        .default_headers(client.headers().clone())
        .timeout(client.timeout())
        // Do not forward account credentials to a redirected endpoint. The
        // session document already supplies the API URL we should address.
        .redirect(reqwest::redirect::Policy::none())
        .build()
        .context("Failed to build Email/set HTTP client")
}

pub(super) async fn send(
    client: &Client,
    http: &reqwest::Client,
    args: Value,
) -> Result<EmailSetResponse> {
    let mut request = serde_json::to_value(super::build_request(client))?;
    request["methodCalls"] = json!([["Email/set", args, "s0"]]);
    let response = http
        .post(client.session().api_url())
        .json(&request)
        .send()
        .await
        .context("Email/set HTTP request failed")?;
    if !response.status().is_success() {
        return Err(jmap_client::Error::Server(response.status().to_string()))
            .context("Email/set HTTP request rejected");
    }
    let response: Response<SingleMethodResponse<EmailSetResponse>> = serde_json::from_slice(
        &response
            .bytes()
            .await
            .context("Failed to read Email/set response")?,
    )
    .context("Invalid Email/set response")?;
    if response.session_state() != client.session().state()
        && let Err(error) = client.refresh_session().await
    {
        // The mutation was already accepted. Session refresh failure must
        // not turn its acknowledgement into a failed mutation attempt.
        tracing::warn!("Email/set session refresh failed: {}", error);
    }
    let mut responses = response.unwrap_method_responses();
    if responses.len() != 1 {
        anyhow::bail!("Email/set expected exactly one method response");
    }
    match responses.pop().unwrap() {
        SingleMethodResponse::Ok((name, response, call_id)) => {
            if name != "Email/set" || call_id != "s0" {
                anyhow::bail!("Email/set response has unexpected method or call id");
            }
            if response.account_id() != Some(client.default_account_id()) {
                anyhow::bail!("Email/set response has missing or mismatched account id");
            }
            Ok(response)
        }
        SingleMethodResponse::Error((_, error, call_id)) => {
            if call_id != "s0" {
                anyhow::bail!("Email/set error has unexpected call id");
            }
            Err(jmap_client::Error::from(error)).context("Email/set method failed")
        }
    }
}
