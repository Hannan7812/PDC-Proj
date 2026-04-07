package pdc.common;

public class TaskDescriptor {
    private String taskId;
    private String filePath;
    private int startLine;
    private int endLine;
    private int attempt;
    private ComputeMode mode;

    public TaskDescriptor() {
    }

    public TaskDescriptor(String taskId, String filePath, int startLine, int endLine, int attempt, ComputeMode mode) {
        this.taskId = taskId;
        this.filePath = filePath;
        this.startLine = startLine;
        this.endLine = endLine;
        this.attempt = attempt;
        this.mode = mode;
    }

    public String getTaskId() {
        return taskId;
    }

    public void setTaskId(String taskId) {
        this.taskId = taskId;
    }

    public String getFilePath() {
        return filePath;
    }

    public void setFilePath(String filePath) {
        this.filePath = filePath;
    }

    public int getStartLine() {
        return startLine;
    }

    public void setStartLine(int startLine) {
        this.startLine = startLine;
    }

    public int getEndLine() {
        return endLine;
    }

    public void setEndLine(int endLine) {
        this.endLine = endLine;
    }

    public int getAttempt() {
        return attempt;
    }

    public void setAttempt(int attempt) {
        this.attempt = attempt;
    }

    public ComputeMode getMode() {
        return mode;
    }

    public void setMode(ComputeMode mode) {
        this.mode = mode;
    }
}
