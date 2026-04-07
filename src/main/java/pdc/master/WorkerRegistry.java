package pdc.master;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class WorkerRegistry {
    private final Map<String, Long> heartbeats;

    public WorkerRegistry() {
        this.heartbeats = new ConcurrentHashMap<>();
    }

    public void register(String workerId) {
        heartbeats.put(workerId, System.currentTimeMillis());
    }

    public void heartbeat(String workerId) {
        heartbeats.put(workerId, System.currentTimeMillis());
    }

    public int workerCount() {
        return heartbeats.size();
    }
}
