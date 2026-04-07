package pdc.master;

import pdc.common.TaskDescriptor;

import java.io.BufferedReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class DataService {
    private final Map<String, TaskDescriptor> taskIndex;

    public DataService() {
        this.taskIndex = new ConcurrentHashMap<>();
    }

    public void registerTask(TaskDescriptor descriptor) {
        taskIndex.put(descriptor.getTaskId(), descriptor);
    }

    public TaskDescriptor getTask(String taskId) {
        return taskIndex.get(taskId);
    }

    public List<String> readLinesForTask(String taskId) {
        TaskDescriptor descriptor = taskIndex.get(taskId);
        if (descriptor == null) {
            return Collections.emptyList();
        }
        Path path = Path.of(descriptor.getFilePath());
        int start = Math.max(0, descriptor.getStartLine());
        int end = Math.max(start, descriptor.getEndLine());
        List<String> selected = new ArrayList<>(Math.max(1, end - start + 1));

        try {
            try (BufferedReader reader = Files.newBufferedReader(path)) {
                String line;
                int lineNumber = 0;
                while ((line = reader.readLine()) != null) {
                    if (lineNumber > end) {
                        break;
                    }
                    if (lineNumber >= start) {
                        selected.add(line);
                    }
                    lineNumber++;
                }
            }
            return selected;
        } catch (IOException e) {
            throw new IllegalStateException("Failed to read file for task " + taskId, e);
        }
    }
}
