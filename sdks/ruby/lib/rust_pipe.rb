require "json"
require "websocket-client-simple"
require "securerandom"

module RustPipe
  class Worker
    def initialize(url:, handlers:, worker_id: nil, max_concurrency: 10, heartbeat_interval: 5)
      @url = url
      @handlers = handlers
      @worker_id = worker_id || "worker-ruby-#{SecureRandom.hex(4)}"
      @max_concurrency = max_concurrency
      @heartbeat_interval = heartbeat_interval
      @active_tasks = 0
      @start_time = Time.now
      @running = false
      @mutex = Mutex.new
      @semaphore = Queue.new
      max_concurrency.times { @semaphore << true }
    end

    def start
      @running = true
      connect
    end

    def stop
      @running = false
      @ws&.close
    end

    private

    def connect
      worker = self
      @ws = WebSocket::Client::Simple.connect(@url)

      @ws.on :open do
        worker.send(:register)
        worker.send(:start_heartbeat)
      end

      @ws.on :message do |msg|
        worker.send(:handle_message, msg.data)
      end

      @ws.on :close do
        worker.send(:reconnect) if worker.instance_variable_get(:@running)
      end

      sleep # Keep main thread alive
    end

    def register
      send_msg({
        type: "WorkerRegister",
        registration: {
          workerId: @worker_id,
          supportedTasks: @handlers.keys,
          maxConcurrency: @max_concurrency,
          language: "Ruby",
        }
      })
    end

    def start_heartbeat
      Thread.new do
        while @running
          sleep @heartbeat_interval
          send_msg({
            type: "Heartbeat",
            payload: {
              workerId: @worker_id,
              activeTasks: @active_tasks,
              capacity: @max_concurrency,
              uptimeSeconds: (Time.now - @start_time).to_i,
            }
          })
        end
      end
    end

    def handle_message(raw)
      msg = JSON.parse(raw, symbolize_names: true)
      case msg[:type]
      when "TaskDispatch"
        task = msg[:task]
        Thread.new { execute_task(task) }
      when "Shutdown"
        stop
      end
    rescue JSON::ParserError
      nil
    end

    def execute_task(task)
      @semaphore.pop
      @mutex.synchronize { @active_tasks += 1 }
      start_time = Time.now

      handler = @handlers[task[:taskType]]
      unless handler
        send_result(task[:id], "Failed", nil,
          { code: "UNKNOWN_TASK_TYPE", message: "No handler for: #{task[:taskType]}", retryable: false }, 0)
        return
      end

      result = handler.call(task)
      duration = ((Time.now - start_time) * 1000).to_i
      send_result(task[:id], "Completed", result, nil, duration)
    rescue => e
      duration = ((Time.now - start_time) * 1000).to_i
      send_result(task[:id], "Failed", nil,
        { code: "HANDLER_ERROR", message: e.message, retryable: true }, duration)
    ensure
      @mutex.synchronize { @active_tasks -= 1 }
      @semaphore << true
    end

    def send_result(task_id, status, payload, error, duration_ms)
      send_msg({
        type: "TaskResult",
        result: {
          taskId: task_id,
          status: status,
          payload: payload,
          error: error,
          durationMs: duration_ms,
          workerId: @worker_id,
        }
      })
    end

    def send_msg(msg)
      @ws&.send(JSON.generate(msg))
    end
  end
end
