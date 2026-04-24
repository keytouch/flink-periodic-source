package net.krmo.flink.source.periodic;

import java.io.IOException;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.connector.source.SplitsAssignment;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import net.krmo.flink.source.periodic.event.Event;

public class PeriodicEnumerator<OUT extends Serializable>
        implements SplitEnumerator<PeriodicSplit, PeriodicEnumeratorState> {

    private static final Logger LOG = LoggerFactory.getLogger(PeriodicEnumerator.class);

    private final SplitEnumeratorContext<PeriodicSplit> context;
    private final PeriodicEnumeratorState state;
    private final SourceSupplier<OUT> supplier;
    private final long initialDelayMillis;
    private final long discoverPeriodMillis;
    private boolean initialized;

    public PeriodicEnumerator(
            SplitEnumeratorContext<PeriodicSplit> context,
            PeriodicEnumeratorState state,
            SourceSupplier<OUT> supplier,
            long initialDelayMillis,
            long discoverPeriodMillis) {
        this.context = context;
        this.state = state;
        this.supplier = supplier;
        this.initialDelayMillis = initialDelayMillis;
        this.discoverPeriodMillis = discoverPeriodMillis;
    }

    @Override
    public void start() {
        try {
            supplier.open();
        } catch (Exception e) {
            throw new RuntimeException("Error while initializing SourceSupplier", e);
        }
    }

    @Override
    public void handleSplitRequest(int subtaskId, String requesterHostname) {
        // No reader should request splits spontaneously
    }

    @Override
    public void addSplitsBack(List<PeriodicSplit> splits, int subtaskId) {
        LOG.warn(
                "failed splits: {} sent back from subtask: {}, splits will be spreaded out on next schedule",
                splits, subtaskId);
    }

    @Override
    public void addReader(int subtaskId) {
        if (!initialized
                && context.registeredReadersOfAttempts().size() == context.currentParallelism()) {
            LOG.info("All readers are online, start splits assigning");
            initialized = true;
            context.callAsync(() -> supplier.get(),
                    this::processSupplierResult,
                    initialDelayMillis,
                    discoverPeriodMillis);
        }
    }

    @Override
    public PeriodicEnumeratorState snapshotState(long checkpointId) throws Exception {
        return state;
    }

    @Override
    public void close() throws IOException {
    }

    private void processSupplierResult(Collection<SourceSupplierOutput<OUT>> value, Throwable error) {
        if (error != null) {
            LOG.error("Failed to enumerate supplier result", error);
            return;
        }

        List<Integer> subtaskIds = new ArrayList<>(context.registeredReadersOfAttempts().keySet());
        if (subtaskIds.size() == 0) {
            LOG.info("No registered Readers, skipping...");
            return;
        }

        Map<Integer, List<PeriodicSplit>> assignment = new HashMap<>();
        subtaskIds.forEach(subtaskId -> {
            assignment.put(subtaskId, Collections.singletonList(new PeriodicSplit("subtasksplit_" + subtaskId)));
        });

        int outputIndex = 0;
        for (SourceSupplierOutput<OUT> supplierOutput : value) {
            int subtaskId = subtaskIds.get(outputIndex % subtaskIds.size());
            assignment.get(subtaskId).get(0).addEvent(new Event<>(
                    supplierOutput.id,
                    supplierOutput.value,
                    supplierOutput.periodicConfig));
            outputIndex++;
        }

        // log
        assignment.forEach((subtaskId, splits) -> {
            LOG.info("assigning {} to subtask {}", splits.get(0), subtaskId);
        });

        context.assignSplits(new SplitsAssignment<>(assignment));
    }
}
