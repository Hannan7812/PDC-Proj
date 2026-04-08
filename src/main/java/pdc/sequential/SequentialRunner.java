package pdc.sequential;

import pdc.common.ComputeMode;
import pdc.common.TaskDescriptor;
import pdc.common.TaskResult;
import pdc.compute.ComputeKernel;
import pdc.master.ResultAggregator;
import pdc.master.TaskPartitioner;

import java.io.BufferedWriter;
import java.io.FileWriter;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class SequentialRunner {

    public SequentialResult run(Path dataPath, int chunkLines) {
        Map<String, Integer> wordCount = runSinglePass(dataPath, chunkLines, ComputeMode.WORD_COUNT);
        Map<String, Map<String, List<Integer>>> invertedIndex = runSinglePass(dataPath, chunkLines, ComputeMode.INVERTED_INDEX);

        try (BufferedWriter wcWriter = new BufferedWriter(new FileWriter("sequential-wor-count.txt"));
             BufferedWriter idxWriter = new BufferedWriter(new FileWriter("sequential-inv-index.txt"))) {
            wcWriter.write(String.valueOf(wordCount));
            idxWriter.write(String.valueOf(invertedIndex));
        } catch (IOException e) {
            System.err.println("Failed to write sequential outputs: " + e.getMessage());
        }

        return new SequentialResult(wordCount, invertedIndex);
    }

    @SuppressWarnings("unchecked")
    private <T> T runSinglePass(Path dataPath, int chunkLines, ComputeMode mode) {
        TaskPartitioner partitioner = new TaskPartitioner();
        List<TaskDescriptor> tasks = partitioner.buildTasks(dataPath, chunkLines, mode);
        ResultAggregator aggregator = new ResultAggregator();
        ComputeKernel kernel = ComputeKernel.forMode(mode);
        ExecutorService executor = Executors.newFixedThreadPool(1);

        try {
            for (TaskDescriptor task : tasks) {
                List<String> lines = readLinesForTask(task);
                TaskResult partial = kernel.compute("sequential", task, lines, executor, 1);
                partial.setCompletedTaskIds(List.of(task.getTaskId()));
                partial.setChunkCount(1);
                aggregator.record(partial);
            }
        } finally {
            executor.shutdownNow();
        }
        if (mode == ComputeMode.WORD_COUNT) {
            return (T) aggregator.aggregateWordCount();
        }
        return (T) aggregator.aggregateInvertedIndex();
    }

    private List<String> readLinesForTask(TaskDescriptor task) {
        int start = Math.max(0, task.getStartLine());
        int end = Math.max(start, task.getEndLine());
        List<String> selected = new java.util.ArrayList<>(Math.max(1, end - start + 1));

        try (java.io.BufferedReader reader = Files.newBufferedReader(Path.of(task.getFilePath()))) {
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
            throw new IllegalStateException("Sequential task read failed for " + task.getTaskId(), e);
        }
    }

    public record SequentialResult(
            Map<String, Integer> wordCount,
            Map<String, Map<String, List<Integer>>> invertedIndex
    ) {
    }
}
