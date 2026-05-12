/**
 * Minimal TypeScript worker for e2e testing.
 * Connects to rust-pipe dispatcher via WebSocket.
 */
import WebSocket from 'ws';

const DISPATCHER_URL = process.env.DISPATCHER_URL ?? 'ws://127.0.0.1:19900';
const WORKER_ID = `ts-test-${Date.now()}`;

const ws = new WebSocket(DISPATCHER_URL);

ws.on('open', () => {
  // Register
  ws.send(JSON.stringify({
    type: 'WorkerRegister',
    registration: {
      workerId: WORKER_ID,
      supportedTasks: ['echo', 'add-numbers', 'slow-task'],
      maxConcurrency: 5,
      language: 'TypeScript',
    },
  }));

  console.error(`[ts-worker] Registered as ${WORKER_ID}`);
});

ws.on('message', async (data: Buffer) => {
  const msg = JSON.parse(data.toString());

  if (msg.type === 'TaskDispatch') {
    const task = msg.task;
    console.error(`[ts-worker] Received task: ${task.taskType} (${task.id})`);

    let result: any;
    const start = Date.now();

    try {
      switch (task.taskType) {
        case 'echo':
          result = { echoed: task.payload };
          break;
        case 'add-numbers':
          result = { sum: task.payload.a + task.payload.b };
          break;
        case 'slow-task':
          await new Promise(r => setTimeout(r, task.payload.delay_ms ?? 500));
          result = { completed: true };
          break;
        default:
          throw new Error(`Unknown task type: ${task.taskType}`);
      }

      ws.send(JSON.stringify({
        type: 'TaskResult',
        result: {
          taskId: task.id,
          status: 'completed',
          payload: result,
          durationMs: Date.now() - start,
          workerId: WORKER_ID,
        },
      }));
    } catch (err: any) {
      ws.send(JSON.stringify({
        type: 'TaskResult',
        result: {
          taskId: task.id,
          status: 'failed',
          error: { code: 'HANDLER_ERROR', message: err.message, retryable: false },
          durationMs: Date.now() - start,
          workerId: WORKER_ID,
        },
      }));
    }
  } else if (msg.type === 'Shutdown') {
    console.error('[ts-worker] Shutdown received');
    ws.close();
    process.exit(0);
  }
});

ws.on('error', (err) => {
  console.error(`[ts-worker] Error: ${err.message}`);
  process.exit(1);
});
