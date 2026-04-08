package pdc;

import pdc.common.TaskDescriptor;
import pdc.config.AppConfig;
import pdc.master.TaskPartitioner;
import pdc.metrics.MetricsRecorder;
import pdc.sequential.SequentialRunner;

import java.nio.file.Path;
import java.util.List;
import java.util.Map;

public class AppSequential {
    public static void main(String[] args) {
        AppConfig config = AppConfig.load();
        Path dataPath = Path.of("/data").toFile().exists() ? Path.of("/data") : Path.of("data");

        SequentialRunner runner = new SequentialRunner();
        long start = System.currentTimeMillis();
        SequentialRunner.SequentialResult result = runner.run(dataPath, config.taskChunkLines());
        long elapsed = System.currentTimeMillis() - start;

        MetricsRecorder metrics = new MetricsRecorder(config.metricsOutputDir());
        metrics.writeTimeline("sequential", "total", elapsed);
        TaskPartitioner partitioner = new TaskPartitioner();
        List<TaskDescriptor> tasks = partitioner.buildTasks(dataPath, config.taskChunkLines(), pdc.common.ComputeMode.WORD_COUNT);
        int taskCount = tasks.size();
        metrics.writeRunCompletion(
            "sequential",
            "BOTH",
            true,
            elapsed,
            1,
            1,
            taskCount,
            taskCount
        );

        long wordCountCardinality = result.wordCount().size();
        long invIndexEntries = invertedIndexEntries(result.invertedIndex());
        System.out.println("Sequential mode=BOTH elapsedMs=" + elapsed
                + " wordCountCardinality=" + wordCountCardinality
                + " invertedIndexEntries=" + invIndexEntries);
    }

    private static long invertedIndexEntries(Map<String, Map<String, List<Integer>>> index) {
        long entries = 0L;
        for (Map<String, List<Integer>> fileMap : index.values()) {
            for (List<Integer> positions : fileMap.values()) {
                entries += positions.size();
            }
        }
        return entries;
    }
}
