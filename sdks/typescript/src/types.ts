export interface Task<T = unknown> {
  id: string;
  taskType: string;
  payload: T;
  metadata: TaskMetadata;
}

export interface TaskMetadata {
  createdAt: string;
  timeoutMs: number;
  priority: 'low' | 'normal' | 'high' | 'critical';
  retryCount: number;
  maxRetries: number;
  traceId?: string;
}

export interface TaskResult<T = unknown> {
  taskId: string;
  status: 'completed' | 'failed';
  payload?: T;
  error?: TaskError;
  durationMs: number;
  workerId: string;
}

export interface TaskError {
  code: string;
  message: string;
  retryable: boolean;
}

export type TaskHandler<TInput = unknown, TOutput = unknown> = (
  task: Task<TInput>
) => Promise<TOutput>;

export type Message =
  | { type: 'TaskDispatch'; task: Task }
  | { type: 'TaskResult'; result: TaskResult }
  | { type: 'Heartbeat'; payload: HeartbeatPayload }
  | { type: 'HeartbeatAck' }
  | { type: 'WorkerRegister'; registration: WorkerRegistration }
  | { type: 'WorkerRegistered'; workerId: string }
  | { type: 'Backpressure'; signal: BackpressureSignal }
  | { type: 'Kill'; taskId: string; reason: string }
  | { type: 'Shutdown'; graceful: boolean };

export interface HeartbeatPayload {
  workerId: string;
  activeTasks: number;
  capacity: number;
  uptimeSeconds: number;
}

export interface WorkerRegistration {
  workerId: string;
  supportedTasks: string[];
  maxConcurrency: number;
  language: 'TypeScript' | 'Python' | 'Go' | string;
}

export interface BackpressureSignal {
  workerId: string;
  currentLoad: number;
  shouldThrottle: boolean;
}
