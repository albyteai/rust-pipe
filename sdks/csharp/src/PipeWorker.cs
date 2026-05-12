using System.Collections.Concurrent;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Serialization;

namespace RustPipe;

public delegate Task<object?> TaskHandler(PipeTask task);

public class PipeWorker : IAsyncDisposable
{
    private readonly string _url;
    private readonly string _workerId;
    private readonly Dictionary<string, TaskHandler> _handlers;
    private readonly int _maxConcurrency;
    private readonly TimeSpan _heartbeatInterval;
    private readonly SemaphoreSlim _semaphore;
    private readonly ClientWebSocket _ws = new();
    private readonly CancellationTokenSource _cts = new();
    private readonly DateTime _startTime = DateTime.UtcNow;
    private int _activeTasks;

    private static readonly JsonSerializerOptions JsonOpts = new()
    {
        PropertyNamingPolicy = JsonNamingPolicy.CamelCase,
        DefaultIgnoreCondition = JsonIgnoreCondition.WhenWritingNull,
    };

    private PipeWorker(string url, string workerId, Dictionary<string, TaskHandler> handlers,
        int maxConcurrency, TimeSpan heartbeatInterval)
    {
        _url = url;
        _workerId = workerId;
        _handlers = handlers;
        _maxConcurrency = maxConcurrency;
        _heartbeatInterval = heartbeatInterval;
        _semaphore = new SemaphoreSlim(maxConcurrency);
    }

    public static Builder CreateBuilder(string url) => new(url);

    public async Task StartAsync()
    {
        await _ws.ConnectAsync(new Uri(_url), _cts.Token);
        await RegisterAsync();
        _ = HeartbeatLoopAsync();
        await ReceiveLoopAsync();
    }

    public async Task StopAsync()
    {
        _cts.Cancel();
        if (_ws.State == WebSocketState.Open)
            await _ws.CloseAsync(WebSocketCloseStatus.NormalClosure, "shutdown", CancellationToken.None);
    }

    private async Task RegisterAsync()
    {
        var msg = new
        {
            type = "WorkerRegister",
            registration = new
            {
                workerId = _workerId,
                supportedTasks = _handlers.Keys.ToArray(),
                maxConcurrency = _maxConcurrency,
                language = "CSharp",
            }
        };
        await SendAsync(msg);
    }

    private async Task HeartbeatLoopAsync()
    {
        while (!_cts.IsCancellationRequested)
        {
            await Task.Delay(_heartbeatInterval, _cts.Token);
            var msg = new
            {
                type = "Heartbeat",
                payload = new
                {
                    workerId = _workerId,
                    activeTasks = _activeTasks,
                    capacity = _maxConcurrency,
                    uptimeSeconds = (int)(DateTime.UtcNow - _startTime).TotalSeconds,
                }
            };
            await SendAsync(msg);
        }
    }

    private async Task ReceiveLoopAsync()
    {
        var buffer = new byte[65536];
        while (!_cts.IsCancellationRequested && _ws.State == WebSocketState.Open)
        {
            var result = await _ws.ReceiveAsync(buffer, _cts.Token);
            if (result.MessageType == WebSocketMessageType.Close) break;

            var raw = Encoding.UTF8.GetString(buffer, 0, result.Count);
            var doc = JsonDocument.Parse(raw);
            var msgType = doc.RootElement.GetProperty("type").GetString();

            switch (msgType)
            {
                case "TaskDispatch":
                    var taskElem = doc.RootElement.GetProperty("task");
                    var task = new PipeTask(
                        taskElem.GetProperty("id").GetString()!,
                        taskElem.GetProperty("taskType").GetString()!,
                        taskElem.GetProperty("payload")
                    );
                    _ = ExecuteTaskAsync(task);
                    break;
                case "Shutdown":
                    await StopAsync();
                    break;
            }
        }
    }

    private async Task ExecuteTaskAsync(PipeTask task)
    {
        await _semaphore.WaitAsync(_cts.Token);
        Interlocked.Increment(ref _activeTasks);
        var start = DateTime.UtcNow;

        try
        {
            if (!_handlers.TryGetValue(task.TaskType, out var handler))
            {
                await SendResultAsync(task.Id, "Failed", null,
                    new { code = "UNKNOWN_TASK_TYPE", message = $"No handler for: {task.TaskType}", retryable = false }, 0);
                return;
            }

            var result = await handler(task);
            var duration = (int)(DateTime.UtcNow - start).TotalMilliseconds;
            await SendResultAsync(task.Id, "Completed", result, null, duration);
        }
        catch (Exception ex)
        {
            var duration = (int)(DateTime.UtcNow - start).TotalMilliseconds;
            await SendResultAsync(task.Id, "Failed", null,
                new { code = "HANDLER_ERROR", message = ex.Message, retryable = true }, duration);
        }
        finally
        {
            Interlocked.Decrement(ref _activeTasks);
            _semaphore.Release();
        }
    }

    private async Task SendResultAsync(string taskId, string status, object? payload, object? error, int durationMs)
    {
        var msg = new
        {
            type = "TaskResult",
            result = new
            {
                taskId,
                status,
                payload,
                error,
                durationMs,
                workerId = _workerId,
            }
        };
        await SendAsync(msg);
    }

    private async Task SendAsync(object message)
    {
        var json = JsonSerializer.Serialize(message, JsonOpts);
        var bytes = Encoding.UTF8.GetBytes(json);
        await _ws.SendAsync(bytes, WebSocketMessageType.Text, true, _cts.Token);
    }

    public async ValueTask DisposeAsync()
    {
        await StopAsync();
        _ws.Dispose();
        _cts.Dispose();
    }

    public class Builder
    {
        private readonly string _url;
        private string? _workerId;
        private readonly Dictionary<string, TaskHandler> _handlers = new();
        private int _maxConcurrency = 10;
        private TimeSpan _heartbeatInterval = TimeSpan.FromSeconds(5);

        internal Builder(string url) { _url = url; }

        public Builder WorkerId(string id) { _workerId = id; return this; }
        public Builder MaxConcurrency(int n) { _maxConcurrency = n; return this; }
        public Builder HeartbeatInterval(TimeSpan interval) { _heartbeatInterval = interval; return this; }
        public Builder Handle(string taskType, TaskHandler handler) { _handlers[taskType] = handler; return this; }

        public PipeWorker Build()
        {
            var workerId = _workerId ?? $"worker-csharp-{Guid.NewGuid().ToString()[..8]}";
            return new PipeWorker(_url, workerId, _handlers, _maxConcurrency, _heartbeatInterval);
        }
    }
}

public record PipeTask(string Id, string TaskType, JsonElement Payload);
