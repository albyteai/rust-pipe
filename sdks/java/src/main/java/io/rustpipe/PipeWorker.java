package io.rustpipe;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import org.java_websocket.client.WebSocketClient;
import org.java_websocket.handshake.ServerHandshake;

import java.net.URI;
import java.time.Instant;
import java.util.*;
import java.util.concurrent.*;
import java.util.concurrent.atomic.AtomicInteger;

public class PipeWorker {
    private final String url;
    private final String workerId;
    private final Map<String, TaskHandler> handlers;
    private final int maxConcurrency;
    private final long heartbeatIntervalMs;
    private final Gson gson = new Gson();
    private final ExecutorService executor;
    private final AtomicInteger activeTasks = new AtomicInteger(0);
    private final long startTime = System.currentTimeMillis();

    private WebSocketClient ws;
    private ScheduledExecutorService heartbeatScheduler;
    private volatile boolean running = false;

    public PipeWorker(Builder builder) {
        this.url = builder.url;
        this.workerId = builder.workerId != null ? builder.workerId :
                "worker-java-" + UUID.randomUUID().toString().substring(0, 8);
        this.handlers = builder.handlers;
        this.maxConcurrency = builder.maxConcurrency;
        this.heartbeatIntervalMs = builder.heartbeatIntervalMs;
        this.executor = Executors.newFixedThreadPool(maxConcurrency);
    }

    public void start() throws Exception {
        running = true;
        connect();
    }

    public void stop() {
        running = false;
        if (heartbeatScheduler != null) heartbeatScheduler.shutdown();
        if (ws != null) ws.close();
        executor.shutdown();
    }

    private void connect() throws Exception {
        ws = new WebSocketClient(new URI(url)) {
            @Override
            public void onOpen(ServerHandshake handshake) {
                register();
                startHeartbeat();
            }

            @Override
            public void onMessage(String raw) {
                handleMessage(raw);
            }

            @Override
            public void onClose(int code, String reason, boolean remote) {
                if (running) reconnect();
            }

            @Override
            public void onError(Exception ex) {
                System.err.println("[rust-pipe-java] Error: " + ex.getMessage());
            }
        };
        ws.connect();
    }

    private void reconnect() {
        try {
            Thread.sleep(1000);
            connect();
        } catch (Exception e) {
            System.err.println("[rust-pipe-java] Reconnect failed: " + e.getMessage());
        }
    }

    private void register() {
        JsonObject reg = new JsonObject();
        reg.addProperty("type", "WorkerRegister");

        JsonObject registration = new JsonObject();
        registration.addProperty("workerId", workerId);
        registration.add("supportedTasks", gson.toJsonTree(new ArrayList<>(handlers.keySet())));
        registration.addProperty("maxConcurrency", maxConcurrency);
        registration.addProperty("language", "Java");
        reg.add("registration", registration);

        ws.send(gson.toJson(reg));
    }

    private void startHeartbeat() {
        heartbeatScheduler = Executors.newSingleThreadScheduledExecutor();
        heartbeatScheduler.scheduleAtFixedRate(() -> {
            JsonObject hb = new JsonObject();
            hb.addProperty("type", "Heartbeat");

            JsonObject payload = new JsonObject();
            payload.addProperty("workerId", workerId);
            payload.addProperty("activeTasks", activeTasks.get());
            payload.addProperty("capacity", maxConcurrency);
            payload.addProperty("uptimeSeconds", (System.currentTimeMillis() - startTime) / 1000);
            hb.add("payload", payload);

            ws.send(gson.toJson(hb));
        }, heartbeatIntervalMs, heartbeatIntervalMs, TimeUnit.MILLISECONDS);
    }

    private void handleMessage(String raw) {
        JsonObject msg = gson.fromJson(raw, JsonObject.class);
        String type = msg.get("type").getAsString();

        switch (type) {
            case "TaskDispatch" -> {
                JsonObject taskJson = msg.getAsJsonObject("task");
                Task task = new Task(
                        taskJson.get("id").getAsString(),
                        taskJson.get("taskType").getAsString(),
                        taskJson.get("payload")
                );
                executor.submit(() -> executeTask(task));
            }
            case "Shutdown" -> stop();
        }
    }

    private void executeTask(Task task) {
        activeTasks.incrementAndGet();
        long start = System.currentTimeMillis();

        try {
            TaskHandler handler = handlers.get(task.taskType());
            if (handler == null) {
                sendResult(task.id(), "Failed", null,
                        new TaskError("UNKNOWN_TASK_TYPE", "No handler for: " + task.taskType(), false),
                        0);
                return;
            }

            Object result = handler.handle(task);
            long duration = System.currentTimeMillis() - start;
            sendResult(task.id(), "Completed", result, null, duration);
        } catch (Exception e) {
            long duration = System.currentTimeMillis() - start;
            sendResult(task.id(), "Failed", null,
                    new TaskError("HANDLER_ERROR", e.getMessage(), true), duration);
        } finally {
            activeTasks.decrementAndGet();
        }
    }

    private void sendResult(String taskId, String status, Object payload, TaskError error, long durationMs) {
        JsonObject msg = new JsonObject();
        msg.addProperty("type", "TaskResult");

        JsonObject result = new JsonObject();
        result.addProperty("taskId", taskId);
        result.addProperty("status", status);
        if (payload != null) result.add("payload", gson.toJsonTree(payload));
        if (error != null) result.add("error", gson.toJsonTree(error));
        result.addProperty("durationMs", durationMs);
        result.addProperty("workerId", workerId);
        msg.add("result", result);

        ws.send(gson.toJson(msg));
    }

    public static Builder builder(String url) {
        return new Builder(url);
    }

    public static class Builder {
        private final String url;
        private String workerId;
        private final Map<String, TaskHandler> handlers = new HashMap<>();
        private int maxConcurrency = 10;
        private long heartbeatIntervalMs = 5000;

        public Builder(String url) { this.url = url; }

        public Builder workerId(String id) { this.workerId = id; return this; }
        public Builder maxConcurrency(int n) { this.maxConcurrency = n; return this; }
        public Builder heartbeatInterval(long ms) { this.heartbeatIntervalMs = ms; return this; }
        public Builder handler(String taskType, TaskHandler h) { this.handlers.put(taskType, h); return this; }

        public PipeWorker build() { return new PipeWorker(this); }
    }

    public record Task(String id, String taskType, Object payload) {}
    public record TaskError(String code, String message, boolean retryable) {}

    @FunctionalInterface
    public interface TaskHandler {
        Object handle(Task task) throws Exception;
    }
}
