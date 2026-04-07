package pdc.master;

import pdc.config.AppConfig;

import java.io.IOException;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class MasterServer {
    private final MasterRuntime runtime;
    private final WorkerRegistry workerRegistry;
    private final ExecutorService connectionPool;

    public MasterServer(AppConfig config) {
        this.runtime = new MasterRuntime(config);
        this.workerRegistry = new WorkerRegistry();
        this.connectionPool = Executors.newCachedThreadPool();
    }

    public void start() {
        runtime.initializeTasks();

        try (ServerSocket serverSocket = new ServerSocket(runtime.config().masterPort())) {
            System.out.println("Master listening on port " + runtime.config().masterPort());
            while (true) {
                Socket socket = serverSocket.accept();
                connectionPool.submit(new WorkerConnectionHandler(socket, runtime, workerRegistry));
            }
        } catch (IOException e) {
            throw new IllegalStateException("Master server failed", e);
        }
    }
}
