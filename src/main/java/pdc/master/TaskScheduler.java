package pdc.master;

import pdc.common.TaskDescriptor;
import pdc.common.TaskBatchAssignment;
import pdc.common.TaskState;
import pdc.common.TaskStatus;

import java.util.Comparator;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.PriorityQueue;
import java.util.concurrent.ConcurrentHashMap;

public class TaskScheduler {
    private final PriorityQueue<TaskState> pendingQueue;
    private final Map<String, TaskState> stateByTaskId;
    private final int maxRetries;
    private final long taskTimeoutMs;
    private final long maxRuntimeMs;
    private final int minBatchSize;
    private final int maxBatchSize;
    private final double overheadToComputeIncreaseThreshold;
    private final double computeToOverheadDecreaseThreshold;
    private final long startedAt;
    private final Map<String, WorkerAdaptiveState> workerAdaptiveState;

    public TaskScheduler(int maxRetries,
                         long taskTimeoutMs,
                         long maxRuntimeMs,
                         int minBatchSize,
                         int maxBatchSize,
                         double overheadToComputeIncreaseThreshold,
                         double computeToOverheadDecreaseThreshold) {
        this.maxRetries = maxRetries;
        this.taskTimeoutMs = taskTimeoutMs;
        this.maxRuntimeMs = maxRuntimeMs;
        this.minBatchSize = Math.max(1, minBatchSize);
        this.maxBatchSize = Math.max(this.minBatchSize, maxBatchSize);
        this.overheadToComputeIncreaseThreshold = Math.max(1.0, overheadToComputeIncreaseThreshold);
        this.computeToOverheadDecreaseThreshold = Math.max(1.0, computeToOverheadDecreaseThreshold);
        this.startedAt = System.currentTimeMillis();
        this.pendingQueue = new PriorityQueue<>(Comparator
                .comparing((TaskState ts) -> ts.getDescriptor().getFilePath())
                .thenComparingInt(ts -> ts.getDescriptor().getStartLine())
                .thenComparing(ts -> ts.getDescriptor().getTaskId()));
        this.stateByTaskId = new ConcurrentHashMap<>();
        this.workerAdaptiveState = new ConcurrentHashMap<>();
    }

    public synchronized void addTask(TaskDescriptor descriptor) {
        TaskState state = new TaskState(descriptor);
        pendingQueue.offer(state);
        stateByTaskId.put(descriptor.getTaskId(), state);
    }

    public synchronized Optional<TaskBatchAssignment> assignNextTask(String workerId) {
        if (isRuntimeExceeded()) {
            return Optional.empty();
        }
        requeueTimedOutTasks();
        TaskState next = pendingQueue.poll();
        if (next == null) {
            return Optional.empty();
        }

        WorkerAdaptiveState adaptive = workerAdaptiveState.computeIfAbsent(
                workerId,
                ignored -> new WorkerAdaptiveState(this.minBatchSize)
        );
        int targetBatchSize = adaptive.batchSize;

        List<TaskDescriptor> assignments = new ArrayList<>();
        markAssigned(next, workerId);
        assignments.add(next.getDescriptor());

        TaskState previous = next;
        while (assignments.size() < targetBatchSize) {
            TaskState candidate = pendingQueue.peek();
            if (candidate == null || !isContiguous(previous, candidate)) {
                break;
            }
            candidate = pendingQueue.poll();
            markAssigned(candidate, workerId);
            assignments.add(candidate.getDescriptor());
            previous = candidate;
        }

        return Optional.of(new TaskBatchAssignment(assignments));
    }

    public synchronized void completeTask(String taskId) {
        TaskState state = stateByTaskId.get(taskId);
        if (state != null && state.getStatus() != TaskStatus.DONE) {
            state.setStatus(TaskStatus.DONE);
        }
    }

    public synchronized void completeTasks(List<String> taskIds) {
        for (String taskId : taskIds) {
            completeTask(taskId);
        }
    }

    public synchronized void observeBatchResult(String workerId, List<String> taskIds, long computeMs) {
        if (taskIds == null || taskIds.isEmpty()) {
            return;
        }
        long startedAt = Long.MAX_VALUE;
        for (String taskId : taskIds) {
            TaskState state = stateByTaskId.get(taskId);
            if (state == null) {
                continue;
            }
            startedAt = Math.min(startedAt, state.getAssignedAt());
        }
        if (startedAt == Long.MAX_VALUE) {
            return;
        }

        long turnaroundMs = Math.max(0L, System.currentTimeMillis() - startedAt);
        long overheadMs = Math.max(0L, turnaroundMs - Math.max(0L, computeMs));

        WorkerAdaptiveState adaptive = workerAdaptiveState.computeIfAbsent(
                workerId,
                ignored -> new WorkerAdaptiveState(this.minBatchSize)
        );
        adaptive.observe(computeMs, overheadMs);

        if (adaptive.overheadEwma > adaptive.computeEwma * overheadToComputeIncreaseThreshold) {
            adaptive.batchSize = Math.min(maxBatchSize, adaptive.batchSize * 2);
        } else if (adaptive.computeEwma > adaptive.overheadEwma * computeToOverheadDecreaseThreshold) {
            adaptive.batchSize = Math.max(minBatchSize, adaptive.batchSize - 1);
        }
    }

    public synchronized int totalTasks() {
        return stateByTaskId.size();
    }

    public synchronized int completedTasks() {
        return (int) stateByTaskId.values().stream().filter(ts -> ts.getStatus() == TaskStatus.DONE).count();
    }

    public synchronized boolean allCompleted() {
        return !stateByTaskId.isEmpty() && completedTasks() == totalTasks();
    }

    public synchronized boolean isTerminalState() {
        if (stateByTaskId.isEmpty()) {
            return true;
        }
        boolean allDoneOrFailed = stateByTaskId.values().stream()
                .allMatch(ts -> ts.getStatus() == TaskStatus.DONE || ts.getStatus() == TaskStatus.FAILED);
        return allDoneOrFailed || isRuntimeExceeded();
    }

    public synchronized void requeueForRetry(String taskId) {
        TaskState state = stateByTaskId.get(taskId);
        if (state == null || state.getStatus() == TaskStatus.DONE) {
            return;
        }
        if (!canRetry(state)) {
            state.setStatus(TaskStatus.FAILED);
            return;
        }
        state.incrementRetries();
        state.getDescriptor().setAttempt(state.getRetries());
        state.setStatus(TaskStatus.PENDING);
        state.setAssignedWorkerId(null);
        state.setAssignedAt(0L);
        pendingQueue.offer(state);
    }

    private void requeueTimedOutTasks() {
        long now = System.currentTimeMillis();
        stateByTaskId.values().forEach(state -> {
            if (state.getStatus() == TaskStatus.IN_PROGRESS && (now - state.getAssignedAt()) > taskTimeoutMs) {
                requeueForRetry(state.getDescriptor().getTaskId());
            }
        });
    }

    private boolean canRetry(TaskState state) {
        if (isRuntimeExceeded()) {
            return false;
        }
        if (maxRetries < 0) {
            return true;
        }
        return state.getRetries() < maxRetries;
    }

    private boolean isRuntimeExceeded() {
        return (System.currentTimeMillis() - startedAt) > maxRuntimeMs;
    }

    private void markAssigned(TaskState state, String workerId) {
        state.setStatus(TaskStatus.IN_PROGRESS);
        state.setAssignedWorkerId(workerId);
        state.setAssignedAt(System.currentTimeMillis());
    }

    private boolean isContiguous(TaskState previous, TaskState next) {
        TaskDescriptor left = previous.getDescriptor();
        TaskDescriptor right = next.getDescriptor();
        return left.getMode() == right.getMode()
                && left.getFilePath().equals(right.getFilePath())
                && (left.getEndLine() + 1) == right.getStartLine();
    }

    private static class WorkerAdaptiveState {
        private int batchSize;
        private double computeEwma;
        private double overheadEwma;

        private WorkerAdaptiveState(int initialBatchSize) {
            this.batchSize = initialBatchSize;
            this.computeEwma = 0D;
            this.overheadEwma = 0D;
        }

        private void observe(long computeMs, long overheadMs) {
            final double alpha = 0.2D;
            double c = Math.max(0D, computeMs);
            double o = Math.max(0D, overheadMs);
            this.computeEwma = computeEwma == 0D ? c : ((1 - alpha) * computeEwma + alpha * c);
            this.overheadEwma = overheadEwma == 0D ? o : ((1 - alpha) * overheadEwma + alpha * o);
        }
    }
}
