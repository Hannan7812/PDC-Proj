package pdc.compute;

import pdc.common.TaskDescriptor;
import pdc.common.TaskResult;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;

public class InvertedIndexKernel implements ComputeKernel {
    // Creating an inverted index for a given list of lines
    @Override
    public TaskResult compute(String workerId,
            TaskDescriptor descriptor,
            List<String> lines,
            ExecutorService executorService,
            int threadCount) {
        TaskResult result = new TaskResult();
        result.setTaskId(descriptor.getTaskId());
        result.setWorkerId(workerId);
        result.setMode(descriptor.getMode());

        List<Callable<Map<String, Map<String, List<Integer>>>>> jobs = new ArrayList<>();

        int chunk = Math.max(1, (int) Math.ceil((double) lines.size() / threadCount));
        for (int start = 0; start < lines.size(); start += chunk) {
            final int rangeStart = start;
            final int rangeEnd = Math.min(lines.size(), start + chunk);
            final int globalStart = descriptor.getStartLine() + rangeStart;
            jobs.add(() -> indexRange(descriptor.getFilePath(), lines.subList(rangeStart, rangeEnd), globalStart));
        }

        try {
            List<Future<Map<String, Map<String, List<Integer>>>>> futures = executorService.invokeAll(jobs);
            Map<String, Map<String, List<Integer>>> merged = new HashMap<>();
            for (Future<Map<String, Map<String, List<Integer>>>> future : futures) {
                Map<String, Map<String, List<Integer>>> part = future.get();
                merge(merged, part);
            }
            result.setInvertedIndex(merged);
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Inverted index interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Inverted index failed", e);
        }
    }

    private Map<String, Map<String, List<Integer>>> indexRange(String filePath, List<String> lines, int startLine) {
        Map<String, Map<String, List<Integer>>> part = new HashMap<>();
        for (int local = 0; local < lines.size(); local++) {
            int lineNumber = startLine + local;
            for (String token : Tokenizer.tokens(lines.get(local))) {
                Map<String, List<Integer>> fileMap = part.computeIfAbsent(token, k -> new HashMap<>());
                List<Integer> positions = fileMap.computeIfAbsent(filePath, k -> new ArrayList<>());
                positions.add(lineNumber);
            }
        }
        return part;
    }

    private void merge(Map<String, Map<String, List<Integer>>> target,
            Map<String, Map<String, List<Integer>>> source) {
        source.forEach((term, fileMap) -> {
            Map<String, List<Integer>> targetFileMap = target.computeIfAbsent(term, k -> new HashMap<>());
            fileMap.forEach((file, positions) -> {
                List<Integer> targetPositions = targetFileMap.computeIfAbsent(file, k -> new ArrayList<>());
                targetPositions.addAll(positions);
                targetPositions.sort(Integer::compareTo);
            });
        });
    }
}
