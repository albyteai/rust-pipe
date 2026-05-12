defmodule RustPipe.Worker do
  use WebSockex
  require Logger

  defstruct [:worker_id, :handlers, :max_concurrency, :active_tasks, :start_time]

  def start_link(url, opts) do
    worker_id = Keyword.get(opts, :worker_id, "worker-elixir-#{:crypto.strong_rand_bytes(4) |> Base.encode16(case: :lower)}")
    handlers = Keyword.fetch!(opts, :handlers)
    max_concurrency = Keyword.get(opts, :max_concurrency, 10)

    state = %__MODULE__{
      worker_id: worker_id,
      handlers: handlers,
      max_concurrency: max_concurrency,
      active_tasks: 0,
      start_time: System.monotonic_time(:second)
    }

    WebSockex.start_link(url, __MODULE__, state, name: __MODULE__)
  end

  @impl true
  def handle_connect(_conn, state) do
    register(state)
    schedule_heartbeat(state)
    {:ok, state}
  end

  @impl true
  def handle_frame({:text, raw}, state) do
    case Jason.decode(raw) do
      {:ok, %{"type" => "TaskDispatch", "task" => task}} ->
        spawn(fn -> execute_task(task, state) end)
        {:ok, state}

      {:ok, %{"type" => "Shutdown"}} ->
        {:close, state}

      _ ->
        {:ok, state}
    end
  end

  @impl true
  def handle_info(:heartbeat, state) do
    payload = %{
      type: "Heartbeat",
      payload: %{
        workerId: state.worker_id,
        activeTasks: state.active_tasks,
        capacity: state.max_concurrency,
        uptimeSeconds: System.monotonic_time(:second) - state.start_time
      }
    }
    frame = {:text, Jason.encode!(payload)}
    schedule_heartbeat(state)
    {:reply, frame, state}
  end

  defp register(state) do
    msg = %{
      type: "WorkerRegister",
      registration: %{
        workerId: state.worker_id,
        supportedTasks: Map.keys(state.handlers),
        maxConcurrency: state.max_concurrency,
        language: "Elixir"
      }
    }
    WebSockex.send_frame(self(), {:text, Jason.encode!(msg)})
  end

  defp schedule_heartbeat(_state) do
    Process.send_after(self(), :heartbeat, 5_000)
  end

  defp execute_task(task, state) do
    task_id = task["id"]
    task_type = task["taskType"]
    start = System.monotonic_time(:millisecond)

    handler = Map.get(state.handlers, task_type)

    result =
      if handler do
        try do
          output = handler.(task["payload"])
          duration = System.monotonic_time(:millisecond) - start
          %{taskId: task_id, status: "Completed", payload: output, durationMs: duration, workerId: state.worker_id}
        rescue
          e ->
            duration = System.monotonic_time(:millisecond) - start
            %{taskId: task_id, status: "Failed",
              error: %{code: "HANDLER_ERROR", message: Exception.message(e), retryable: true},
              durationMs: duration, workerId: state.worker_id}
        end
      else
        %{taskId: task_id, status: "Failed",
          error: %{code: "UNKNOWN_TASK_TYPE", message: "No handler for: #{task_type}", retryable: false},
          durationMs: 0, workerId: state.worker_id}
      end

    msg = %{type: "TaskResult", result: result}
    WebSockex.send_frame(__MODULE__, {:text, Jason.encode!(msg)})
  end
end
