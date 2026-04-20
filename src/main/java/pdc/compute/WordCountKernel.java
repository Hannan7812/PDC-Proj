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

public class WordCountKernel implements ComputeKernel {
    // Counting words in a given list of lines
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

        List<Callable<Map<String, Integer>>> jobs = new ArrayList<>();

        int chunk = Math.max(1, (int) Math.ceil((double) lines.size() / threadCount));
        for (int start = 0; start < lines.size(); start += chunk) {
            final int rangeStart = start;
            final int rangeEnd = Math.min(lines.size(), start + chunk);
            jobs.add(() -> countRange(lines.subList(rangeStart, rangeEnd)));
        }

        try {
            List<Future<Map<String, Integer>>> futures = executorService.invokeAll(jobs);
            Map<String, Integer> merged = new HashMap<>();
            for (Future<Map<String, Integer>> future : futures) {
                Map<String, Integer> part = future.get();
                part.forEach((word, count) -> merged.merge(word, count, Integer::sum));
            }
            result.setWordCounts(merged);
            return result;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new IllegalStateException("Word count interrupted", e);
        } catch (ExecutionException e) {
            throw new IllegalStateException("Word count failed", e);
        }
    }

    private Map<String, Integer> countRange(List<String> lines) {
        Map<String, Integer> counts = new HashMap<>();
        for (String line : lines) {
            for (String token : Tokenizer.tokens(line)) {
                counts.merge(token, 1, Integer::sum);
            }
        }
        return counts;
    }
}
