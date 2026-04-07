package pdc;

import pdc.config.AppConfig;
import pdc.worker.WorkerClient;

public class AppWorker {
    public static void main(String[] args) {
        String workerId = args.length > 0 ? args[0] : "worker-" + System.currentTimeMillis();
        AppConfig config = AppConfig.load();
        WorkerClient worker = new WorkerClient(workerId, config);
        worker.start();
    }
}
