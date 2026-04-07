package pdc.master;

import pdc.common.ComputeMode;
import pdc.common.TaskResult;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

public class ResultAggregator {
    private final Set<String> recordedTaskIds;
    private final Map<String, Integer> wordCountAggregate;
    private final Map<String, Map<String, List<Integer>>> invertedIndexAggregate;

    public ResultAggregator() {
        this.recordedTaskIds = new HashSet<>();
        this.wordCountAggregate = new HashMap<>();
        this.invertedIndexAggregate = new HashMap<>();
    }

    public synchronized void record(TaskResult result) {
        List<String> taskIds = result.getCompletedTaskIds();
        if (taskIds == null || taskIds.isEmpty()) {
            taskIds = List.of(result.getTaskId());
        }
        if (taskIds.stream().allMatch(recordedTaskIds::contains)) {
            return;
        }
        recordedTaskIds.addAll(taskIds.stream().filter(id -> id != null && !id.isBlank()).collect(Collectors.toSet()));
        if (result.getMode() == ComputeMode.INVERTED_INDEX) {
            mergeInvertedIndex(result.getInvertedIndex());
        } else {
            mergeWordCounts(result.getWordCounts());
        }
    }

    public synchronized Map<String, Integer> aggregateWordCount() {
        return new HashMap<>(wordCountAggregate);
    }

    public synchronized Map<String, Map<String, List<Integer>>> aggregateInvertedIndex() {
        Map<String, Map<String, List<Integer>>> snapshot = new HashMap<>();
        invertedIndexAggregate.forEach((term, fileMap) -> {
            Map<String, List<Integer>> copied = new HashMap<>();
            fileMap.forEach((file, positions) -> copied.put(file, new ArrayList<>(positions)));
            snapshot.put(term, copied);
        });
        return snapshot;
    }

    public synchronized Object aggregate(ComputeMode mode) {
        if (mode == ComputeMode.INVERTED_INDEX) {
            return aggregateInvertedIndex();
        }
        return aggregateWordCount();
    }

    private void mergeWordCounts(Map<String, Integer> partialCounts) {
        partialCounts.forEach((word, count) -> wordCountAggregate.merge(word, count, Integer::sum));
    }

    private void mergeInvertedIndex(Map<String, Map<String, List<Integer>>> partialIndex) {
        partialIndex.forEach((term, fileMap) -> {
            Map<String, List<Integer>> targetFileMap = invertedIndexAggregate.computeIfAbsent(term, k -> new HashMap<>());
            fileMap.forEach((file, positions) -> {
                List<Integer> targetPositions = targetFileMap.computeIfAbsent(file, k -> new ArrayList<>());
                targetPositions.addAll(positions);
                Collections.sort(targetPositions);
            });
        });
    }
}
