package pdc.common;

import java.util.ArrayList;
import java.util.List;

public class TaskDataResponse {
    private String taskId;
    private List<String> lines;

    public TaskDataResponse() {
        this.lines = new ArrayList<>();
    }

    public TaskDataResponse(String taskId, List<String> lines) {
        this.taskId = taskId;
        this.lines = lines;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public List<String> getLines() {
        return lines;
    }

    public void setLines(List<String> lines) {
        this.lines = lines;
    }
}
