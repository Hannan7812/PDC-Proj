package pdc.common;

public class TaskState {
    private final TaskDescriptor descriptor;
    private volatile TaskStatus status;
    private volatile String assignedWorkerId;
    private volatile long assignedAt;
    private volatile int retries;

    public TaskState(TaskDescriptor descriptor) {
        this.descriptor = descriptor;
        this.status = TaskStatus.PENDING;
        this.retries = descriptor.getAttempt();
    }

    public TaskDescriptor getDescriptor() {
        return descriptor;
    }

    public TaskStatus getStatus() {
        return status;
    }

    public void setStatus(TaskStatus status) {
        this.status = status;
    }

    public String getAssignedWorkerId() {
        return assignedWorkerId;
    }

    public void setAssignedWorkerId(String assignedWorkerId) {
        this.assignedWorkerId = assignedWorkerId;
    }

    public long getAssignedAt() {
        return assignedAt;
    }

    public void setAssignedAt(long assignedAt) {
        this.assignedAt = assignedAt;
    }

    public int getRetries() {
        return retries;
    }

    public void incrementRetries() {
        this.retries++;
    }
}
