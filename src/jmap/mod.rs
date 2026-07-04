pub mod discovery;
pub mod email;
pub mod limits;
pub mod mailbox;
pub mod retry;
pub mod session;
pub mod types;

use jmap_client::URI;
use jmap_client::client::Client;
use jmap_client::core::request::Request;

/// Build a JMAP request declaring only the capabilities jma uses.
///
/// `Client::build` seeds `using` with a version-dependent capability
/// set -- jmap-client 0.4.2 declares all eleven JMAP capabilities,
/// which Fastmail rejects with a 400. jma only calls Core and Mail
/// methods (Mailbox/Email get/set/changes/query/import; blobs travel
/// over the session's HTTP upload/download URLs, not JMAP methods),
/// so pin `using` to those two regardless of the jmap-client version.
/// Every request jma sends is built through here rather than
/// `client.build()` so none inherits the wider default.
pub(crate) fn build_request(client: &Client) -> Request<'_> {
    let mut request = client.build();
    request.using = vec![URI::Core, URI::Mail];
    request
}

#[cfg(test)]
mod tests {
    use super::*;
    use jmap_client::client::Credentials;
    use serde_json::json;
    use wiremock::matchers::{method, path};
    use wiremock::{Mock, MockServer, ResponseTemplate};

    /// A request built through `build_request` declares only Core and
    /// Mail -- never the wider capability set `Client::build` seeds by
    /// default (all eleven in jmap-client 0.4.2), which Fastmail 400s.
    /// The session below advertises extra capabilities precisely so a
    /// regression that let the default through would surface them here.
    #[tokio::test]
    async fn build_request_declares_only_core_and_mail() {
        let server = MockServer::start().await;
        let session = json!({
            "capabilities": {
                "urn:ietf:params:jmap:core": {},
                "urn:ietf:params:jmap:mail": {},
                "urn:ietf:params:jmap:submission": {},
                "urn:ietf:params:jmap:vacationresponse": {},
                "urn:ietf:params:jmap:calendars": {},
            },
            "accounts": {
                "u1": { "name": "t", "isPersonal": true, "isReadOnly": false,
                        "accountCapabilities": { "urn:ietf:params:jmap:mail": {} } }
            },
            "primaryAccounts": { "urn:ietf:params:jmap:mail": "u1" },
            "username": "test@example.com",
            "apiUrl": format!("{}/jmap", server.uri()),
            "downloadUrl": format!("{}/dl/{{accountId}}/{{blobId}}/{{name}}?accept={{type}}", server.uri()),
            "uploadUrl": format!("{}/up/{{accountId}}", server.uri()),
            "eventSourceUrl": format!("{}/es", server.uri()),
            "state": "s",
        });
        Mock::given(method("GET"))
            .and(path("/.well-known/jmap"))
            .respond_with(ResponseTemplate::new(200).set_body_json(session))
            .mount(&server)
            .await;

        let client = Client::new()
            .credentials(Credentials::bearer("t"))
            .connect(&server.uri())
            .await
            .expect("mock session connect");

        assert_eq!(build_request(&client).using, vec![URI::Core, URI::Mail]);
    }
}
