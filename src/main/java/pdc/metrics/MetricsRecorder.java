package pdc.metrics;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;

public class MetricsRecorder {
    private final Path metricsDir;

    public MetricsRecorder(String outputDir) {
        this.metricsDir = Path.of(outputDir);
        try {
            Files.createDirectories(metricsDir);
        } catch (IOException e) {
            throw new IllegalStateException("Failed to create metrics dir", e);
        }
    }

    public void writeSummaryRow(int workers, int threads, String inputSize,
                                long tSeqMs, long tParMs) {
        double speedup = tParMs == 0 ? 0.0 : (double) tSeqMs / tParMs;
        int p = Math.max(1, workers * threads);
        double efficiency = speedup / p;
        double f = estimateParallelFraction(speedup, p);
        double sMax = 1.0 / ((1.0 - f) + (f / p));

        String header = "workers,threads,input_size,t_seq_ms,t_par_ms,speedup,efficiency,parallel_fraction,s_max\n";
        String row = String.format("%d,%d,%s,%d,%d,%.5f,%.5f,%.5f,%.5f%n",
                workers, threads, inputSize, tSeqMs, tParMs, speedup, efficiency, f, sMax);

        Path csv = metricsDir.resolve("performance_summary.csv");
        appendWithHeader(csv, header, row);
    }

    public void writeTimeline(String runId, String phase, long millis) {
        String header = "run_id,phase,duration_ms\n";
        String row = String.format("%s,%s,%d%n", runId, phase, millis);
        Path csv = metricsDir.resolve("timeline.csv");
        appendWithHeader(csv, header, row);
    }

    public void writeRunCompletion(String runType,
                                   String computeMode,
                                   boolean success,
                                   long durationMs,
                                   int workers,
                                   int threadsPerWorker,
                                   int totalTasks,
                                   int completedTasks) {
        String header = "run_type,compute_mode,success,duration_ms,workers,threads_per_worker,total_tasks,completed_tasks\n";
        String row = String.format("%s,%s,%s,%d,%d,%d,%d,%d%n",
                runType,
                computeMode,
                success,
                durationMs,
                workers,
                threadsPerWorker,
                totalTasks,
                completedTasks);
        Path csv = metricsDir.resolve("run_metrics.csv");
        appendWithHeader(csv, header, row);
    }

    private double estimateParallelFraction(double speedup, int p) {
        if (speedup <= 0.0 || p <= 1) {
            return 0.0;
        }
        double invS = 1.0 / speedup;
        return Math.max(0.0, Math.min(1.0, (1.0 - invS) / (1.0 - (1.0 / p))));
    }

    private void appendWithHeader(Path file, String header, String row) {
        try {
            if (!Files.exists(file)) {
                Files.writeString(file, header, StandardOpenOption.CREATE, StandardOpenOption.WRITE);
            }
            Files.writeString(file, row, StandardOpenOption.APPEND);
        } catch (IOException e) {
            throw new IllegalStateException("Failed writing metrics CSV", e);
        }
    }
}
