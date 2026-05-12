import WebSocket from 'ws';
import { randomUUID } from 'crypto';
import type { Task, TaskResult, TaskHandler, Message } from './types.js';

export interface WorkerConfig {
  url: string;
  workerId?: string;
  handlers: Record<string, TaskHandler>;
  maxConcurrency?: number;
  heartbeatIntervalMs?: number;
  reconnectDelayMs?: number;
  maxReconnectAttempts?: number;
  onError?: (error: Error) => void;
}

export function createWorker(config: WorkerConfig) {
  return new PipeWorker(config);
}

class PipeWorker {
  private ws: WebSocket | null = null;
  private config: Required<Omit<WorkerConfig, 'onError'>> & { onError?: (error: Error) => void };
  private activeTasks = 0;
  private heartbeatTimer: ReturnType<typeof setInterval> | null = null;
  private reconnectAttempts = 0;
  private startTime = Date.now();
  private running = false;

  constructor(config: WorkerConfig) {
    this.config = {
      url: config.url,
      workerId: config.workerId ?? `worker-ts-${randomUUID().slice(0, 8)}`,
      handlers: config.handlers,
      maxConcurrency: config.maxConcurrency ?? 10,
      heartbeatIntervalMs: config.heartbeatIntervalMs ?? 5000,
      reconnectDelayMs: config.reconnectDelayMs ?? 1000,
      maxReconnectAttempts: config.maxReconnectAttempts ?? 10,
      onError: config.onError,
    };
  }

  async start(): Promise<void> {
    this.running = true;
    await this.connect();
  }

  async stop(): Promise<void> {
    this.running = false;
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
    }
    if (this.ws) {
      this.ws.close();
    }
  }

  private async connect(): Promise<void> {
    return new Promise((resolve, reject) => {
      this.ws = new WebSocket(this.config.url);

      this.ws.on('open', () => {
        this.reconnectAttempts = 0;
        this.register();
        this.startHeartbeat();
        resolve();
      });

      this.ws.on('message', (data) => {
        this.handleMessage(data.toString());
      });

      this.ws.on('close', () => {
        this.stopHeartbeat();
        if (this.running) {
          this.reconnect();
        }
      });

      this.ws.on('error', (err) => {
        this.config.onError?.(err);
        reject(err);
      });
    });
  }

  private async reconnect(): Promise<void> {
    if (this.reconnectAttempts >= this.config.maxReconnectAttempts) {
      this.config.onError?.(new Error('Max reconnect attempts reached'));
      return;
    }

    this.reconnectAttempts++;
    const delay = this.config.reconnectDelayMs * Math.pow(2, this.reconnectAttempts - 1);
    await new Promise((r) => setTimeout(r, delay));

    try {
      await this.connect();
    } catch {
      // Will retry via close handler
    }
  }

  private register(): void {
    this.send({
      type: 'WorkerRegister',
      registration: {
        workerId: this.config.workerId,
        supportedTasks: Object.keys(this.config.handlers),
        maxConcurrency: this.config.maxConcurrency,
        language: 'TypeScript',
      },
    });
  }

  private startHeartbeat(): void {
    this.heartbeatTimer = setInterval(() => {
      this.send({
        type: 'Heartbeat',
        payload: {
          workerId: this.config.workerId,
          activeTasks: this.activeTasks,
          capacity: this.config.maxConcurrency,
          uptimeSeconds: Math.floor((Date.now() - this.startTime) / 1000),
        },
      });
    }, this.config.heartbeatIntervalMs);
  }

  private stopHeartbeat(): void {
    if (this.heartbeatTimer) {
      clearInterval(this.heartbeatTimer);
      this.heartbeatTimer = null;
    }
  }

  private async handleMessage(raw: string): Promise<void> {
    let message: Message;
    try {
      message = JSON.parse(raw);
    } catch {
      return;
    }

    switch (message.type) {
      case 'TaskDispatch':
        await this.executeTask(message.task);
        break;
      case 'Shutdown':
        if (message.graceful) {
          await this.stop();
        }
        break;
      case 'Kill':
        // TODO: cancel specific task
        break;
    }
  }

  private async executeTask(task: Task): Promise<void> {
    const handler = this.config.handlers[task.taskType];
    if (!handler) {
      this.sendResult({
        taskId: task.id,
        status: 'failed',
        error: {
          code: 'UNKNOWN_TASK_TYPE',
          message: `No handler for task type: ${task.taskType}`,
          retryable: false,
        },
        durationMs: 0,
        workerId: this.config.workerId,
      });
      return;
    }

    this.activeTasks++;
    const start = Date.now();

    // Signal backpressure if at capacity
    if (this.activeTasks >= this.config.maxConcurrency) {
      this.send({
        type: 'Backpressure',
        signal: {
          workerId: this.config.workerId,
          currentLoad: this.activeTasks / this.config.maxConcurrency,
          shouldThrottle: true,
        },
      });
    }

    try {
      const result = await handler(task);
      this.sendResult({
        taskId: task.id,
        status: 'completed',
        payload: result,
        durationMs: Date.now() - start,
        workerId: this.config.workerId,
      });
    } catch (err) {
      this.sendResult({
        taskId: task.id,
        status: 'failed',
        error: {
          code: 'HANDLER_ERROR',
          message: err instanceof Error ? err.message : 'Unknown error',
          retryable: true,
        },
        durationMs: Date.now() - start,
        workerId: this.config.workerId,
      });
    } finally {
      this.activeTasks--;
    }
  }

  private sendResult(result: TaskResult): void {
    this.send({ type: 'TaskResult', result });
  }

  private send(message: Message): void {
    if (this.ws?.readyState === WebSocket.OPEN) {
      this.ws.send(JSON.stringify(message));
    }
  }
}
