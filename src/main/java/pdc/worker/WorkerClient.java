package pdc.worker;

import pdc.common.MessageEnvelope;
import pdc.common.MessageType;
import pdc.common.ProtocolCodec;
import pdc.common.TaskBatchAssignment;
import pdc.common.TaskDescriptor;
import pdc.common.TaskResult;
import pdc.common.WorkerRegistration;
import pdc.config.AppConfig;

import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.EOFException;
import java.io.IOException;
import java.net.Socket;
import java.util.UUID;

public class WorkerClient {
    private final String workerId;
    private final AppConfig config;
    private final WorkerExecutor executor;

    public WorkerClient(String workerId, AppConfig config) {
        this.workerId = workerId;
        this.config = config;
        this.executor = new WorkerExecutor(workerId, config.workerThreads());
    }

    public void start() {
        try (Socket socket = new Socket(config.masterHost(), config.masterPort());
             DataOutputStream output = new DataOutputStream(socket.getOutputStream());
             DataInputStream input = new DataInputStream(socket.getInputStream())) {

            register(output, input);
            runLoop(output, input);
        } catch (EOFException e) {
            System.out.println(workerId + " disconnected: master closed connection");
        } catch (IOException e) {
            throw new IllegalStateException("Worker failed: " + workerId, e);
        }
    }

    private void register(DataOutputStream output, DataInputStream input) throws IOException {
        WorkerRegistration registration = new WorkerRegistration(workerId, config.workerThreads());
        MessageEnvelope request = new MessageEnvelope(MessageType.REGISTER, UUID.randomUUID().toString(), registration);
        ProtocolCodec.writeEnvelope(output, request);
        ProtocolCodec.readEnvelope(input);
    }

    private void runLoop(DataOutputStream output, DataInputStream input) throws IOException {
        while (true) {
            MessageEnvelope taskRequest = new MessageEnvelope(MessageType.TASK_REQUEST, UUID.randomUUID().toString(), workerId);
            ProtocolCodec.writeEnvelope(output, taskRequest);
            MessageEnvelope taskResponse = ProtocolCodec.readEnvelope(input);

            if (taskResponse.getType() == MessageType.SHUTDOWN) {
                System.out.println(workerId + " received shutdown");
                break;
            }

            if (taskResponse.getType() == MessageType.NO_TASK_AVAILABLE) {
                sendHeartbeat(output, input);
                sleep(config.workerIdleBackoffMs());
                continue;
            }

            if (taskResponse.getType() != MessageType.TASK_ASSIGN
                    && taskResponse.getType() != MessageType.TASK_BATCH_ASSIGN) {
                throw new IllegalStateException("Unexpected response: " + taskResponse.getType());
            }

            TaskBatchAssignment assignment;
            if (taskResponse.getType() == MessageType.TASK_BATCH_ASSIGN) {
                assignment = ProtocolCodec.payloadAs(taskResponse.getPayload(), TaskBatchAssignment.class);
            } else {
                TaskDescriptor task = ProtocolCodec.payloadAs(taskResponse.getPayload(), TaskDescriptor.class);
                assignment = new TaskBatchAssignment(java.util.List.of(task));
            }
            TaskResult result = executor.executeBatch(assignment);

            MessageEnvelope resultMessage = new MessageEnvelope(MessageType.RESULT_RETURN, UUID.randomUUID().toString(), result);
            ProtocolCodec.writeEnvelope(output, resultMessage);
            ProtocolCodec.readEnvelope(input);
        }
    }

    private void sendHeartbeat(DataOutputStream output, DataInputStream input) throws IOException {
        MessageEnvelope hb = new MessageEnvelope(MessageType.HEARTBEAT, UUID.randomUUID().toString(), workerId);
        ProtocolCodec.writeEnvelope(output, hb);
        ProtocolCodec.readEnvelope(input);
    }

    private void sleep(long ms) {
        try {
            Thread.sleep(ms);
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        }
    }
}
