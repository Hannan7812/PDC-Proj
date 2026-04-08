package pdc.worker;

import pdc.common.TaskDescriptor;
import pdc.common.TaskResult;
import pdc.common.TaskBatchAssignment;
import pdc.compute.ComputeKernel;
import pdc.common.ComputeMode;

import java.io.IOException;
import java.io.BufferedReader;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executors;

public class WorkerExecutor {
    private final String workerId;
    private final int threadCount;
    private final int wordCountThreads;
    private final int invertedIndexThreads;
    private final ExecutorService wordCountPool;
    private final ExecutorService invertedIndexPool;

    public WorkerExecutor(String workerId, int threadCount) {
        this.workerId = workerId;
        this.threadCount = threadCount;
        this.wordCountThreads = Math.max(1, threadCount / 2);
        this.invertedIndexThreads = Math.max(1, threadCount - wordCountThreads);
        this.wordCountPool = Executors.newFixedThreadPool(wordCountThreads);
        this.invertedIndexPool = Executors.newFixedThreadPool(invertedIndexThreads);

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            wordCountPool.shutdownNow();
            invertedIndexPool.shutdownNow();
        }));
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
        List<String> lines = readLinesForTask(merged);
        long startedAt = System.currentTimeMillis();
        TaskResult result = computeBothModes(merged, lines);
        long computeMs = System.currentTimeMillis() - startedAt;
        result.setCompletedTaskIds(tasks.stream().map(TaskDescriptor::getTaskId).toList());
        result.setChunkCount(tasks.size());
        result.setComputeMs(computeMs);
        result.setTaskId(tasks.get(0).getTaskId());
        return result;
    }

    private TaskResult computeBothModes(TaskDescriptor merged, List<String> lines) {
        TaskDescriptor wcTask = new TaskDescriptor(
                merged.getTaskId(),
                merged.getFilePath(),
                merged.getStartLine(),
                merged.getEndLine(),
                merged.getAttempt(),
                ComputeMode.WORD_COUNT
        );
        TaskDescriptor idxTask = new TaskDescriptor(
                merged.getTaskId(),
                merged.getFilePath(),
                merged.getStartLine(),
                merged.getEndLine(),
                merged.getAttempt(),
                ComputeMode.INVERTED_INDEX
        );

        ComputeKernel wordCountKernel = ComputeKernel.forMode(ComputeMode.WORD_COUNT);
        ComputeKernel invertedIndexKernel = ComputeKernel.forMode(ComputeMode.INVERTED_INDEX);

        CompletableFuture<TaskResult> wcFuture = CompletableFuture.supplyAsync(
                () -> wordCountKernel.compute(workerId, wcTask, lines, wordCountPool, wordCountThreads)
        );
        CompletableFuture<TaskResult> idxFuture = CompletableFuture.supplyAsync(
                () -> invertedIndexKernel.compute(workerId, idxTask, lines, invertedIndexPool, invertedIndexThreads)
        );

        try {
            TaskResult wc = wcFuture.get();
            TaskResult idx = idxFuture.get();

            TaskResult combined = new TaskResult();
            combined.setTaskId(merged.getTaskId());
            combined.setWorkerId(workerId);
            combined.setMode(merged.getMode());
            combined.setWordCounts(wc.getWordCounts());
            combined.setInvertedIndex(idx.getInvertedIndex());
            return combined;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Dual compute interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Dual compute failed", e);
        }
    }

    private List<String> readLinesForTask(TaskDescriptor descriptor) {
        int start = Math.max(0, descriptor.getStartLine());
        int end = Math.max(start, descriptor.getEndLine());
        List<String> selected = new ArrayList<>(Math.max(1, end - start + 1));

        try (BufferedReader reader = Files.newBufferedReader(Path.of(descriptor.getFilePath()))) {
            String line;
            int lineNumber = 0;
            while ((line = reader.readLine()) != null) {
                if (lineNumber > end) {
                    break;
                }
                if (lineNumber >= start) {
                    selected.add(line);
                }
                lineNumber++;
            }
            return selected;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read file range for task " + descriptor.getTaskId(), e);
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
