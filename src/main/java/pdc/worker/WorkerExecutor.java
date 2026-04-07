package pdc.worker;

import pdc.common.TaskDescriptor;
import pdc.common.TaskResult;
import pdc.common.TaskBatchAssignment;
import pdc.compute.ComputeKernel;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class WorkerExecutor {
    private final String workerId;
    private final int threadCount;
    private final ExecutorService computePool;
    private final Map<String, List<String>> fileLineCache;

    public WorkerExecutor(String workerId, int threadCount) {
        this.workerId = workerId;
        this.threadCount = threadCount;
        this.computePool = Executors.newFixedThreadPool(threadCount);
        this.fileLineCache = new ConcurrentHashMap<>();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> computePool.shutdownNow()));
    }

    public TaskResult execute(TaskDescriptor descriptor) {
        return executeBatch(new TaskBatchAssignment(List.of(descriptor)));
    }

    public TaskResult executeBatch(TaskBatchAssignment assignment) {
        List<TaskDescriptor> tasks = assignment.getTasks();
        if (tasks == null || tasks.isEmpty()) {
            throw new IllegalStateException("Received empty task batch");
        }

        TaskDescriptor merged = mergeContiguousTasks(tasks);
        ComputeKernel kernel = ComputeKernel.forMode(merged.getMode());
        List<String> lines = readLinesForTask(merged);
        long startedAt = System.currentTimeMillis();
        TaskResult result = kernel.compute(workerId, merged, lines, computePool, threadCount);
        long computeMs = System.currentTimeMillis() - startedAt;
        result.setCompletedTaskIds(tasks.stream().map(TaskDescriptor::getTaskId).toList());
        result.setChunkCount(tasks.size());
        result.setComputeMs(computeMs);
        result.setTaskId(tasks.get(0).getTaskId());
        return result;
    }

    private List<String> readLinesForTask(TaskDescriptor descriptor) {
        List<String> allLines = fileLineCache.computeIfAbsent(descriptor.getFilePath(), this::readAllLines);
        int start = Math.max(0, descriptor.getStartLine());
        int end = Math.min(allLines.size() - 1, Math.max(start, descriptor.getEndLine()));
        if (allLines.isEmpty() || start > end) {
            return List.of();
        }
        return new ArrayList<>(allLines.subList(start, end + 1));
    }

    private List<String> readAllLines(String filePath) {
        try {
            return Files.readAllLines(Path.of(filePath));
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read file " + filePath, e);
        }
    }

    private TaskDescriptor mergeContiguousTasks(List<TaskDescriptor> tasks) {
        TaskDescriptor first = tasks.get(0);
        TaskDescriptor last = tasks.get(tasks.size() - 1);
        return new TaskDescriptor(
                first.getTaskId(),
                first.getFilePath(),
                first.getStartLine(),
                last.getEndLine(),
                first.getAttempt(),
                first.getMode()
        );
    }
}
