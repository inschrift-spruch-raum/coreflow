use coreflow::{Graph, GraphRunStatus, json};
use serde::{Deserialize, Serialize};

#[derive(Debug, Deserialize)]
struct ExtractInput {
    profile: Profile,
}

#[derive(Debug, Deserialize, Serialize)]
struct Profile {
    email: String,
    name: String,
}

#[derive(Debug, Serialize)]
struct ExtractOutput {
    profile: Profile,
}

#[derive(Debug, Deserialize)]
struct EmailInput {
    recipient: String,
    display_name: String,
}

#[derive(Debug, Serialize, Deserialize, PartialEq, Eq)]
struct EmailOutput {
    sent_to: String,
    greeting: String,
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> coreflow::CoreResult<()> {
    let mut graph = Graph::new();

    graph
        .plugup("extract_user", |input: ExtractInput| async move {
            Ok(ExtractOutput {
                profile: input.profile,
            })
        })?
        .plugup("send_email", |input: EmailInput| async move {
            Ok(EmailOutput {
                sent_to: input.recipient,
                greeting: format!("Hello {}", input.display_name),
            })
        })?
        .plugin("extract_user", "extract_user")?
        .plugin("send_email", "send_email")?
        .flowin(json!({
            "send_email": {
                "recipient": "extract_user.profile.email",
                "display_name": "extract_user.profile.name"
            }
        }))?;

    let result = graph
        .run(json!({
            "profile": {
                "email": "ada@example.com",
                "name": "Ada"
            }
        }))
        .await?;
    let output = result.output().get::<EmailOutput>("send_email")?;

    assert_eq!(result.status, GraphRunStatus::Idle);
    assert_eq!(output.sent_to, "ada@example.com");
    println!("field_flow: {}", output.greeting);

    Ok(())
}
