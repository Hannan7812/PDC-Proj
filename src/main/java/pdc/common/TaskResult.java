package pdc.common;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.ArrayList;

public class TaskResult {
    private String taskId;
    private String workerId;
    private ComputeMode mode;
    private List<String> completedTaskIds;
    private int chunkCount;
    private long computeMs;
    private Map<String, Integer> wordCounts;
    private Map<String, Map<String, List<Integer>>> invertedIndex;

    public TaskResult() {
        this.completedTaskIds = new ArrayList<>();
        this.wordCounts = new HashMap<>();
        this.invertedIndex = new HashMap<>();
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public ComputeMode getMode() {
        return mode;
    }

    public void setMode(ComputeMode mode) {
        this.mode = mode;
    }

    public List<String> getCompletedTaskIds() {
        return completedTaskIds;
    }

    public void setCompletedTaskIds(List<String> completedTaskIds) {
        this.completedTaskIds = completedTaskIds;
    }

    public int getChunkCount() {
        return chunkCount;
    }

    public void setChunkCount(int chunkCount) {
        this.chunkCount = chunkCount;
    }

    public long getComputeMs() {
        return computeMs;
    }

    public void setComputeMs(long computeMs) {
        this.computeMs = computeMs;
    }

    public Map<String, Integer> getWordCounts() {
        return wordCounts;
    }

    public void setWordCounts(Map<String, Integer> wordCounts) {
        this.wordCounts = wordCounts;
    }

    public Map<String, Map<String, List<Integer>>> getInvertedIndex() {
        return invertedIndex;
    }

    public void setInvertedIndex(Map<String, Map<String, List<Integer>>> invertedIndex) {
        this.invertedIndex = invertedIndex;
    }
}
