#!/bin/bash
# Minimal stdio pipe worker — reads JSON tasks from stdin, writes results to stdout.
# This demonstrates that ANY Unix tool can be a rust-pipe worker.
#
# Protocol: one JSON message per line on stdin, one JSON response per line on stdout.
# All fields use camelCase. TaskStatus values: Completed, Failed.

while IFS= read -r line; do
  msg_type=$(echo "$line" | jq -r '.type // empty')

  case "$msg_type" in
    "TaskDispatch")
      task_id=$(echo "$line" | jq -r '.task.id')
      task_type=$(echo "$line" | jq -r '.task.taskType')
      payload=$(echo "$line" | jq -c '.task.payload')

      case "$task_type" in
        "word-count")
          text=$(echo "$payload" | jq -r '.text')
          count=$(echo "$text" | wc -w | tr -d ' ')
          echo "{\"type\":\"TaskResult\",\"result\":{\"taskId\":\"$task_id\",\"status\":\"Completed\",\"payload\":{\"wordCount\":$count},\"durationMs\":1,\"workerId\":\"stdio-bash\"}}"
          ;;
        "disk-usage")
          path=$(echo "$payload" | jq -r '.path // "."')
          usage=$(du -sh "$path" 2>/dev/null | cut -f1)
          echo "{\"type\":\"TaskResult\",\"result\":{\"taskId\":\"$task_id\",\"status\":\"Completed\",\"payload\":{\"usage\":\"$usage\",\"path\":\"$path\"},\"durationMs\":1,\"workerId\":\"stdio-bash\"}}"
          ;;
        "ping")
          host=$(echo "$payload" | jq -r '.host')
          if ping -c 1 -W 2 "$host" >/dev/null 2>&1; then
            echo "{\"type\":\"TaskResult\",\"result\":{\"taskId\":\"$task_id\",\"status\":\"Completed\",\"payload\":{\"reachable\":true,\"host\":\"$host\"},\"durationMs\":1,\"workerId\":\"stdio-bash\"}}"
          else
            echo "{\"type\":\"TaskResult\",\"result\":{\"taskId\":\"$task_id\",\"status\":\"Completed\",\"payload\":{\"reachable\":false,\"host\":\"$host\"},\"durationMs\":1,\"workerId\":\"stdio-bash\"}}"
          fi
          ;;
        *)
          echo "{\"type\":\"TaskResult\",\"result\":{\"taskId\":\"$task_id\",\"status\":\"Failed\",\"error\":{\"code\":\"UNKNOWN_TASK\",\"message\":\"Unknown: $task_type\",\"retryable\":false},\"durationMs\":0,\"workerId\":\"stdio-bash\"}}"
          ;;
      esac
      ;;

    "Shutdown")
      exit 0
      ;;
  esac
done
