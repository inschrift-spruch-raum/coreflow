use boa_engine::{Context, Source};
use coreflow::{Graph, GraphRunStatus, json};
use serde_json::Value;

fn run_js_function(script: &str, input: &Value) -> coreflow::CoreResult<Value> {
    let mut context = Context::default();
    let wrapped = format!(
        "const __coreflow_input = {input};\n{script}\nJSON.stringify(main(__coreflow_input));"
    );
    let result = context
        .eval(Source::from_bytes(&wrapped))
        .map_err(|error| coreflow::CoreError::PlugFailed {
            plug: "js".to_string(),
            message: error.to_string(),
        })?;
    let text = result
        .to_string(&mut context)
        .map_err(|error| coreflow::CoreError::PlugFailed {
            plug: "js".to_string(),
            message: error.to_string(),
        })?
        .to_std_string_escaped();
    serde_json::from_str(&text).map_err(coreflow::CoreError::from)
}

#[tokio::main(flavor = "current_thread")]
async fn main() -> coreflow::CoreResult<()> {
    let mut graph = Graph::new();

    graph
        .plugup("js_echo", |input: Value| async move {
            run_js_function(
                r#"
                function main(input) {
                    return { value: input.value + " via JS" };
                }
                "#,
                &input,
            )
        })?
        .plugin("js_echo", "js_echo")?;

    let result = graph.run(json!({ "value": "hello" })).await?;
    let output = result.output().get::<coreflow::Value>("js_echo")?;

    assert_eq!(result.status, GraphRunStatus::Idle);
    println!("js_surface: {output}");

    Ok(())
}
