package pdc.common;

public class WorkerRegistration {
    private String workerId;
    private int threadCount;

    public WorkerRegistration() {
    }

    public WorkerRegistration(String workerId, int threadCount) {
        this.workerId = workerId;
        this.threadCount = threadCount;
    }

    public String getWorkerId() {
        return workerId;
    }

    public void setWorkerId(String workerId) {
        this.workerId = workerId;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount(int threadCount) {
        this.threadCount = threadCount;
    }
}
