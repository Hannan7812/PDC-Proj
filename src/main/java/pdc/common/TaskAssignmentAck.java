package pdc.common;

public class TaskAssignmentAck {
    private boolean assigned;
    private String message;

    public TaskAssignmentAck() {
    }

    public TaskAssignmentAck(boolean assigned, String message) {
        this.assigned = assigned;
        this.message = message;
    }

    public boolean isAssigned() {
        return assigned;
    }

    public void setAssigned(boolean assigned) {
        this.assigned = assigned;
    }

    public String getMessage() {
        return message;
    }

    public void setMessage(String message) {
        this.message = message;
    }
}
