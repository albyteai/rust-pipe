use rust_pipe::schema::Task;
use rust_pipe::transport::Message;
use serde_json::json;

#[test]
fn test_message_serialization_format() {
    let task = Task::new("word-count", json!({"text": "hello world"}));
    let msg = Message::TaskDispatch { task };
    let serialized = serde_json::to_string(&msg).unwrap();

    println!("Serialized: {}", serialized);

    // Must use internally tagged format with camelCase fields
    assert!(serialized.contains(r#""type":"TaskDispatch""#));
    assert!(serialized.contains(r#""task":"#));
    assert!(serialized.contains(r#""taskType":"word-count""#));
}

#[test]
fn test_message_deserialization_from_bash() {
    // Bash worker sends with camelCase and enum variant names
    let bash_response = r#"{"type":"TaskResult","result":{"taskId":"550e8400-e29b-41d4-a716-446655440000","status":"Completed","payload":{"wordCount":3},"durationMs":1,"workerId":"stdio-bash"}}"#;
    let parsed: Result<Message, _> = serde_json::from_str(bash_response);
    println!("Parse result: {:?}", parsed);
    if let Err(ref e) = parsed {
        println!("Error: {}", e);
    }
    assert!(parsed.is_ok(), "Failed to parse bash worker response");
}

#[test]
fn test_task_camel_case() {
    let task = Task::new("word-count", json!({"text": "hello"}));
    let serialized = serde_json::to_string(&task).unwrap();
    println!("Task serialized: {}", serialized);
    assert!(
        serialized.contains("taskType"),
        "Should use camelCase: taskType"
    );
    assert!(
        serialized.contains("timeoutMs"),
        "Should use camelCase: timeoutMs"
    );
}

#[test]
fn test_python_worker_response_format() {
    // This is exactly what the Python worker sends (with spaces from json.dumps)
    let py_response = r#"{"type": "TaskResult", "result": {"taskId": "5826ad05-81f3-42fe-b996-0cdf329d10a4", "status": "Completed", "payload": {"echoed": {"data": [1, 2, 3]}}, "durationMs": 5, "workerId": "py-test-123"}}"#;
    let parsed: Result<Message, _> = serde_json::from_str(py_response);
    println!("Python response parse: {:?}", parsed);
    if let Err(ref e) = parsed {
        println!("Error: {}", e);
    }
    assert!(parsed.is_ok(), "Should parse Python worker response");
}
