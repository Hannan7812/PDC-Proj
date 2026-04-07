package pdc.compute;

import pdc.common.ComputeMode;
import pdc.common.TaskDescriptor;
import pdc.common.TaskResult;

import java.util.List;
import java.util.concurrent.ExecutorService;

public interface ComputeKernel {
    TaskResult compute(String workerId, TaskDescriptor descriptor, List<String> lines, ExecutorService executorService, int threadCount);

    static ComputeKernel forMode(ComputeMode mode) {
        if (mode == ComputeMode.INVERTED_INDEX) {
            return new InvertedIndexKernel();
        }
        return new WordCountKernel();
    }
}
