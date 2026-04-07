package pdc.config;

import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Properties;

public class AppConfig {
    private final Properties properties;

    private AppConfig(Properties properties) {
        this.properties = properties;
    }

    public static AppConfig load() {
        Properties props = new Properties();
        Path configPath = Path.of("config", "application.properties");
        if (Files.exists(configPath)) {
            try (InputStream input = Files.newInputStream(configPath)) {
                props.load(input);
            } catch (IOException e) {
                throw new IllegalStateException("Failed to load config/application.properties", e);
            }
        }
        return new AppConfig(props);
    }

    public String masterHost() {
        return properties.getProperty("master.host", "127.0.0.1");
    }

    public int masterPort() {
        return Integer.parseInt(properties.getProperty("master.port", "9000"));
    }

    public int workerThreads() {
        return Integer.parseInt(properties.getProperty("worker.threads", "2"));
    }

    public int taskChunkLines() {
        return Integer.parseInt(properties.getProperty("task.chunk.lines", "1000"));
    }

    public int schedulerMinBatchSize() {
        return Integer.parseInt(properties.getProperty("scheduler.batch.min", "1"));
    }

    public int schedulerMaxBatchSize() {
        return Integer.parseInt(properties.getProperty("scheduler.batch.max", "64"));
    }

    public double schedulerOverheadToComputeIncreaseThreshold() {
        return Double.parseDouble(properties.getProperty("scheduler.batch.increase.overhead_to_compute", "1.15"));
    }

    public double schedulerComputeToOverheadDecreaseThreshold() {
        return Double.parseDouble(properties.getProperty("scheduler.batch.decrease.compute_to_overhead", "2.0"));
    }

    public int maxRetries() {
        return Integer.parseInt(properties.getProperty("scheduler.max.retries", "3"));
    }

    public long taskTimeoutMs() {
        return Long.parseLong(properties.getProperty("scheduler.task.timeout.ms", "20000"));
    }

    public long maxRuntimeMs() {
        return Long.parseLong(properties.getProperty("scheduler.max.runtime.ms", "1200000"));
    }

    public String computeMode() {
        return properties.getProperty("compute.mode", "WORD_COUNT");
    }

    public String metricsOutputDir() {
        return properties.getProperty("metrics.output.dir", "metrics");
    }

    public long workerIdleBackoffMs() {
        return Long.parseLong(properties.getProperty("worker.idle.backoff.ms", "20"));
    }
}
