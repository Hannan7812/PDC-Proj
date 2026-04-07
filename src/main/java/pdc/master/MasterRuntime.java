package pdc.master;

import pdc.common.ComputeMode;
import pdc.common.TaskDescriptor;
import pdc.config.AppConfig;
import pdc.metrics.MetricsRecorder;

import java.nio.file.Path;
import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;

public class MasterRuntime {
    private final AppConfig config;
    private final TaskScheduler scheduler;
    private final DataService dataService;
    private final ResultAggregator aggregator;
    private final MetricsRecorder metricsRecorder;
    private final AtomicBoolean completionWritten;
    private volatile long runStartedAt;
    private volatile ComputeMode computeMode;

    public MasterRuntime(AppConfig config) {
        this.config = config;
        this.scheduler = new TaskScheduler(
            config.maxRetries(),
            config.taskTimeoutMs(),
            config.maxRuntimeMs(),
            config.schedulerMinBatchSize(),
            config.schedulerMaxBatchSize(),
            config.schedulerOverheadToComputeIncreaseThreshold(),
            config.schedulerComputeToOverheadDecreaseThreshold()
        );
        this.dataService = new DataService();
        this.aggregator = new ResultAggregator();
        this.metricsRecorder = new MetricsRecorder(config.metricsOutputDir());
        this.completionWritten = new AtomicBoolean(false);
        this.runStartedAt = System.currentTimeMillis();
        this.computeMode = ComputeMode.from(config.computeMode());
    }

    public void initializeTasks() {
        this.computeMode = ComputeMode.from(config.computeMode());
        this.runStartedAt = System.currentTimeMillis();
        Path absoluteData = Path.of("/data");
        Path fallbackData = Path.of("data");
        Path dataPath = absoluteData.toFile().exists() ? absoluteData : fallbackData;

        TaskPartitioner partitioner = new TaskPartitioner();
        List<TaskDescriptor> tasks = partitioner.buildTasks(dataPath, config.taskChunkLines(), computeMode);
        tasks.forEach(task -> {
            scheduler.addTask(task);
            dataService.registerTask(task);
        });
        System.out.println("Master initialized tasks: " + tasks.size() + " from " + dataPath.toAbsolutePath());
        System.out.println("Time taken to initialize tasks: " + (System.currentTimeMillis() - runStartedAt) + " ms");

        if (tasks.isEmpty()) {
            metricsRecorder.writeRunCompletion(
                    "parallel",
                    computeMode.name(),
                    true,
                    0,
                    0,
                    config.workerThreads(),
                    0,
                    0
            );
            completionWritten.set(true);
        }
    }

    public void writeParallelCompletionIfNeeded(int workerCount) {
        if (!completionWritten.compareAndSet(false, true)) {
            return;
        }
        long elapsedMs = System.currentTimeMillis() - runStartedAt;
        boolean success = scheduler.allCompleted();
        int totalTasks = scheduler.totalTasks();
        int completedTasks = scheduler.completedTasks();
        metricsRecorder.writeTimeline("parallel", "total", elapsedMs);
        metricsRecorder.writeRunCompletion(
                "parallel",
                computeMode.name(),
            success,
                elapsedMs,
                workerCount,
                config.workerThreads(),
            totalTasks,
            completedTasks
        );
        System.out.println("Parallel mode=" + computeMode
            + " success=" + success
            + " elapsedMs=" + elapsedMs
            + " workers=" + workerCount
            + " tasks=" + completedTasks + "/" + totalTasks);
    }

    public TaskScheduler scheduler() {
        return scheduler;
    }

    public DataService dataService() {
        return dataService;
    }

    public ResultAggregator aggregator() {
        return aggregator;
    }

    public AppConfig config() {
        return config;
    }
}
