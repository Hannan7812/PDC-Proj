package pdc.master;

import pdc.common.MessageEnvelope;
import pdc.common.MessageType;
import pdc.common.ProtocolCodec;
import pdc.common.TaskBatchAssignment;
import pdc.common.TaskAssignmentAck;
import pdc.common.TaskDataRequest;
import pdc.common.TaskDataResponse;
import pdc.common.TaskResult;
import pdc.common.WorkerRegistration;

import java.io.BufferedWriter;
import java.io.DataInputStream;
import java.io.DataOutputStream;
import java.io.FileWriter;
import java.io.IOException;
import java.net.Socket;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Collections;
import java.util.UUID;

public class WorkerConnectionHandler implements Runnable {
    private final Socket socket;
    private final MasterRuntime runtime;
    private final WorkerRegistry workerRegistry;

    public WorkerConnectionHandler(Socket socket, MasterRuntime runtime, WorkerRegistry workerRegistry) {
        this.socket = socket;
        this.runtime = runtime;
        this.workerRegistry = workerRegistry;
    }

    @Override
    public void run() {
        try (DataInputStream input = new DataInputStream(socket.getInputStream());
             DataOutputStream output = new DataOutputStream(socket.getOutputStream())) {
            while (true) {
                MessageEnvelope request = ProtocolCodec.readEnvelope(input);
                MessageEnvelope response = process(request);
                ProtocolCodec.writeEnvelope(output, response);

                if (response.getType() == MessageType.SHUTDOWN) {
                    break;
                }
            }
        } catch (IOException e) {
            System.err.println("Connection closed: " + e.getMessage());
        }
    }

    private MessageEnvelope process(MessageEnvelope request) {
        return switch (request.getType()) {
            case REGISTER -> onRegister(request);
            case HEARTBEAT -> onHeartbeat(request);
            case TASK_REQUEST -> onTaskRequest(request);
            case DATA_REQUEST -> onDataRequest(request);
            case RESULT_RETURN -> onResultReturn(request);
            default -> new MessageEnvelope(MessageType.ERROR, request.getRequestId(), "Unsupported message type");
        };
    }

    private MessageEnvelope onRegister(MessageEnvelope request) {
        WorkerRegistration registration = ProtocolCodec.payloadAs(request.getPayload(), WorkerRegistration.class);
        workerRegistry.register(registration.getWorkerId());
        return new MessageEnvelope(MessageType.REGISTER_ACK, request.getRequestId(), new TaskAssignmentAck(true, "Registered"));
    }

    private MessageEnvelope onHeartbeat(MessageEnvelope request) {
        String workerId = String.valueOf(request.getPayload());
        workerRegistry.heartbeat(workerId);
        return new MessageEnvelope(MessageType.HEARTBEAT, request.getRequestId(), "OK");
    }

    private MessageEnvelope onTaskRequest(MessageEnvelope request) {
        String workerId = String.valueOf(request.getPayload());
        Optional<TaskBatchAssignment> maybeBatch = runtime.scheduler().assignNextTask(workerId);
        if (maybeBatch.isEmpty()) {
            if (runtime.scheduler().isTerminalState()) {
                runtime.writeParallelCompletionIfNeeded(workerRegistry.workerCount());
                return new MessageEnvelope(MessageType.SHUTDOWN, request.getRequestId(), "Scheduler reached terminal state");
            }
            return new MessageEnvelope(MessageType.NO_TASK_AVAILABLE, request.getRequestId(), "No task yet");
        }
        return new MessageEnvelope(MessageType.TASK_BATCH_ASSIGN, request.getRequestId(), maybeBatch.get());
    }

    private MessageEnvelope onDataRequest(MessageEnvelope request) {
        TaskDataRequest dataRequest = ProtocolCodec.payloadAs(request.getPayload(), TaskDataRequest.class);
        List<String> lines = runtime.dataService().readLinesForTask(dataRequest.getTaskId());
        TaskDataResponse response = new TaskDataResponse(dataRequest.getTaskId(), lines);
        return new MessageEnvelope(MessageType.DATA_RESPONSE, request.getRequestId(), response);
    }

    private MessageEnvelope onResultReturn(MessageEnvelope request) {
        TaskResult result = ProtocolCodec.payloadAs(request.getPayload(), TaskResult.class);
        runtime.aggregator().record(result);
        List<String> completedTaskIds = result.getCompletedTaskIds();
        if (completedTaskIds == null || completedTaskIds.isEmpty()) {
            completedTaskIds = Collections.singletonList(result.getTaskId());
        }
        runtime.scheduler().observeBatchResult(result.getWorkerId(), completedTaskIds, result.getComputeMs());
        runtime.scheduler().completeTasks(completedTaskIds);

        if (runtime.scheduler().allCompleted()) {
            Map<String, Integer> finalWordCount = runtime.aggregator().aggregateWordCount();
            Map<String, Map<String, List<Integer>>> finalInvertedIndex = runtime.aggregator().aggregateInvertedIndex();
            System.out.println("Final word count cardinality=" + finalWordCount.size());
            System.out.println("Final inverted index entries=" + invertedIndexEntries(finalInvertedIndex));
            runtime.writeParallelCompletionIfNeeded(workerRegistry.workerCount());
            try (BufferedWriter wcWriter = new BufferedWriter(new FileWriter("parallel_word_count.txt"));
                 BufferedWriter idxWriter = new BufferedWriter(new FileWriter("parallel_inv_index.txt"))) {
                wcWriter.write(String.valueOf(finalWordCount));
                idxWriter.write(String.valueOf(finalInvertedIndex));
            }
            catch (IOException e) {
                System.err.println("Failed to write parallel output: " + e.getMessage());
            }
        }
        return new MessageEnvelope(MessageType.RESULT_RETURN, UUID.randomUUID().toString(), "ACK");
    }

    private long invertedIndexEntries(Map<String, Map<String, List<Integer>>> index) {
        long entries = 0L;
        for (Map<String, List<Integer>> fileMap : index.values()) {
            for (List<Integer> positions : fileMap.values()) {
                entries += positions.size();
            }
        }
        return entries;
    }
}
