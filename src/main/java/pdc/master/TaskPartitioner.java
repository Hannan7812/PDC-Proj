package pdc.master;

import pdc.common.ComputeMode;
import pdc.common.TaskDescriptor;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.w3c.dom.css.Counter;

public class TaskPartitioner {
    // Scans a directory for text files and breaks them down into smaller tasks

    public List<TaskDescriptor> buildTasks(Path dataDir, int chunkLines, ComputeMode mode) {
        List<Path> files = listTextFiles(dataDir);
        AtomicInteger idCounter = new AtomicInteger();
        List<TaskDescriptor> tasks = new ArrayList<>();

        // Get all the .txt files from the directory
        for (Path file : files) {
            int lineCount = safeLineCount(file);
            for (int start = 0; start < lineCount; start += chunkLines) {
                int end = Math.min(start + chunkLines - 1, lineCount - 1);
                String taskId = String.format("T%06d", idCounter.incrementAndGet());
                tasks.add(new TaskDescriptor(taskId, file.toAbsolutePath().toString(), start, end, 0, mode));
            }
        }
        return tasks;
    }

    private List<Path> listTextFiles(Path dataDir) {
        // Counter to generate unique IDs for each task
        if (!Files.exists(dataDir)) {
            return List.of();
        }
        try (Stream<Path> stream = Files.list(dataDir)) {
            return stream
                    .filter(path -> path.getFileName().toString().endsWith(".txt"))
                    .sorted()
                    .collect(Collectors.toList());
        } catch (IOException e) {
            throw new IllegalStateException("Failed to list data dir: " + dataDir, e);
        }
    }

    private int safeLineCount(Path file) {
        try (Stream<String> lines = Files.lines(file)) {
            return (int) lines.count();
        } catch (IOException e) {
            throw new IllegalStateException("Failed to count lines in " + file, e);
        }
    }
}
