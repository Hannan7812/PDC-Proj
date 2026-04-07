package pdc;

import pdc.common.ComputeMode;
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
        ComputeMode mode = ComputeMode.from(config.computeMode());
        Path dataPath = Path.of("/data").toFile().exists() ? Path.of("/data") : Path.of("data");

        SequentialRunner runner = new SequentialRunner();
        long start = System.currentTimeMillis();
        Object result = runner.run(dataPath, config.taskChunkLines(), mode);
        long elapsed = System.currentTimeMillis() - start;

        MetricsRecorder metrics = new MetricsRecorder(config.metricsOutputDir());
        metrics.writeTimeline("sequential", "total", elapsed);
        TaskPartitioner partitioner = new TaskPartitioner();
        List<TaskDescriptor> tasks = partitioner.buildTasks(dataPath, config.taskChunkLines(), mode);
        int taskCount = tasks.size();
        metrics.writeRunCompletion(
            "sequential",
            mode.name(),
            true,
            elapsed,
            1,
            1,
            taskCount,
            taskCount
        );

        long resultCardinality = resultCardinality(result, mode);
        System.out.println("Sequential mode=" + mode + " elapsedMs=" + elapsed + " resultCardinality=" + resultCardinality);
    }

    @SuppressWarnings("unchecked")
    private static long resultCardinality(Object result, ComputeMode mode) {
        if (mode == ComputeMode.WORD_COUNT) {
            return ((Map<String, Integer>) result).size();
        }
        long entries = 0L;
        Map<String, Map<String, List<Integer>>> index = (Map<String, Map<String, List<Integer>>>) result;
        for (Map<String, List<Integer>> fileMap : index.values()) {
            for (List<Integer> positions : fileMap.values()) {
                entries += positions.size();
            }
        }
        return entries;
    }
}
