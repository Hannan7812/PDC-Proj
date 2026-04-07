package pdc.common;

import java.util.ArrayList;
import java.util.List;

public class TaskBatchAssignment {
    private List<TaskDescriptor> tasks;

    public TaskBatchAssignment() {
        this.tasks = new ArrayList<>();
    }

    public TaskBatchAssignment(List<TaskDescriptor> tasks) {
        this.tasks = tasks;
    }

    public List<TaskDescriptor> getTasks() {
        return tasks;
    }

    public void setTasks(List<TaskDescriptor> tasks) {
        this.tasks = tasks;
    }
}
