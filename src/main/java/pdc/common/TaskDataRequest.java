package pdc.common;

public class TaskDataRequest {
    private String taskId;

    public TaskDataRequest() {
    }

    public TaskDataRequest(String taskId) {
        this.taskId = taskId;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }
}
