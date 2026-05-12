<?php

namespace RustPipe;

use WebSocket\Client;
use Ramsey\Uuid\Uuid;

class PipeWorker
{
    private string $url;
    private string $workerId;
    private array $handlers;
    private int $maxConcurrency;
    private float $heartbeatInterval;
    private int $activeTasks = 0;
    private float $startTime;
    private ?Client $ws = null;
    private bool $running = false;

    public function __construct(
        string $url,
        array $handlers,
        ?string $workerId = null,
        int $maxConcurrency = 10,
        float $heartbeatInterval = 5.0,
    ) {
        $this->url = $url;
        $this->handlers = $handlers;
        $this->workerId = $workerId ?? 'worker-php-' . substr(Uuid::uuid4()->toString(), 0, 8);
        $this->maxConcurrency = $maxConcurrency;
        $this->heartbeatInterval = $heartbeatInterval;
        $this->startTime = microtime(true);
    }

    public function start(): void
    {
        $this->running = true;
        $this->connect();
    }

    public function stop(): void
    {
        $this->running = false;
        $this->ws?->close();
    }

    private function connect(): void
    {
        $this->ws = new Client($this->url);
        $this->register();

        $lastHeartbeat = time();

        while ($this->running) {
            try {
                $raw = $this->ws->receive();
                if ($raw) {
                    $this->handleMessage($raw);
                }
            } catch (\Exception $e) {
                if ($this->running) {
                    sleep(1);
                    $this->ws = new Client($this->url);
                    $this->register();
                }
            }

            if (time() - $lastHeartbeat >= $this->heartbeatInterval) {
                $this->sendHeartbeat();
                $lastHeartbeat = time();
            }
        }
    }

    private function register(): void
    {
        $this->send([
            'type' => 'WorkerRegister',
            'registration' => [
                'workerId' => $this->workerId,
                'supportedTasks' => array_keys($this->handlers),
                'maxConcurrency' => $this->maxConcurrency,
                'language' => 'PHP',
            ],
        ]);
    }

    private function sendHeartbeat(): void
    {
        $this->send([
            'type' => 'Heartbeat',
            'payload' => [
                'workerId' => $this->workerId,
                'activeTasks' => $this->activeTasks,
                'capacity' => $this->maxConcurrency,
                'uptimeSeconds' => (int)(microtime(true) - $this->startTime),
            ],
        ]);
    }

    private function handleMessage(string $raw): void
    {
        $msg = json_decode($raw, true);
        if (!$msg || !isset($msg['type'])) return;

        match ($msg['type']) {
            'TaskDispatch' => $this->executeTask($msg['task']),
            'Shutdown' => $this->stop(),
            default => null,
        };
    }

    private function executeTask(array $task): void
    {
        $this->activeTasks++;
        $start = microtime(true);
        $taskId = $task['id'];
        $taskType = $task['taskType'];

        try {
            $handler = $this->handlers[$taskType] ?? null;
            if (!$handler) {
                $this->sendResult($taskId, 'Failed', null, [
                    'code' => 'UNKNOWN_TASK_TYPE',
                    'message' => "No handler for: $taskType",
                    'retryable' => false,
                ], 0);
                return;
            }

            $result = $handler($task);
            $duration = (int)((microtime(true) - $start) * 1000);
            $this->sendResult($taskId, 'Completed', $result, null, $duration);
        } catch (\Exception $e) {
            $duration = (int)((microtime(true) - $start) * 1000);
            $this->sendResult($taskId, 'Failed', null, [
                'code' => 'HANDLER_ERROR',
                'message' => $e->getMessage(),
                'retryable' => true,
            ], $duration);
        } finally {
            $this->activeTasks--;
        }
    }

    private function sendResult(string $taskId, string $status, mixed $payload, ?array $error, int $durationMs): void
    {
        $result = [
            'taskId' => $taskId,
            'status' => $status,
            'durationMs' => $durationMs,
            'workerId' => $this->workerId,
        ];
        if ($payload !== null) $result['payload'] = $payload;
        if ($error !== null) $result['error'] = $error;

        $this->send(['type' => 'TaskResult', 'result' => $result]);
    }

    private function send(array $message): void
    {
        $this->ws?->text(json_encode($message));
    }
}
