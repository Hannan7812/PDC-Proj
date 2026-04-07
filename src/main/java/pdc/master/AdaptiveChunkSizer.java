package pdc.master;

import java.util.concurrent.atomic.AtomicInteger;

public class AdaptiveChunkSizer {
    private final int minChunk;
    private final int maxChunk;
    private final AtomicInteger currentChunk;

    public AdaptiveChunkSizer(int initialChunk, int minChunk, int maxChunk) {
        this.currentChunk = new AtomicInteger(initialChunk);
        this.minChunk = minChunk;
        this.maxChunk = maxChunk;
    }

    public int current() {
        return currentChunk.get();
    }

    public void observe(long taskDurationMs, long timeoutMs) {
        if (taskDurationMs < timeoutMs / 4) {
            currentChunk.updateAndGet(prev -> Math.min(maxChunk, prev + Math.max(1, prev / 10)));
            return;
        }
        if (taskDurationMs > timeoutMs / 2) {
            currentChunk.updateAndGet(prev -> Math.max(minChunk, prev - Math.max(1, prev / 10)));
        }
    }
}
