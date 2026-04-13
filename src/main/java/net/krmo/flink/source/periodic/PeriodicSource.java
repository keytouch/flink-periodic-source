package net.krmo.flink.source.periodic;

import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.core.io.SimpleVersionedSerializer;

import net.krmo.flink.source.SimpleSerializer;
import net.krmo.flink.source.periodic.reader.PeriodicReader;

public class PeriodicSource<OUT> implements Source<OUT, PeriodicSplit<OUT>, PeriodicEnumeratorState> {

    private final SourceSupplier<OUT> supplier;
    private final long initialDelayMillis;
    private final long discoverPeriodMillis;

    public PeriodicSource(
            SourceSupplier<OUT> supplier,
            long initialDelayMillis,
            long discoverPeriodMillis) {
        this.supplier = supplier;
        this.initialDelayMillis = initialDelayMillis;
        this.discoverPeriodMillis = discoverPeriodMillis;
    }

    @Override
    public Boundedness getBoundedness() {
        return Boundedness.CONTINUOUS_UNBOUNDED;
    }

    @Override
    public SourceReader<OUT, PeriodicSplit<OUT>> createReader(SourceReaderContext readerContext) throws Exception {
        return new PeriodicReader<>(readerContext);
    }

    @Override
    public SplitEnumerator<PeriodicSplit<OUT>, PeriodicEnumeratorState> createEnumerator(
            SplitEnumeratorContext<PeriodicSplit<OUT>> enumContext)
            throws Exception {
        return new PeriodicEnumerator<>(
                enumContext,
                new PeriodicEnumeratorState(),
                supplier,
                initialDelayMillis,
                discoverPeriodMillis);
    }

    @Override
    public SplitEnumerator<PeriodicSplit<OUT>, PeriodicEnumeratorState> restoreEnumerator(
            SplitEnumeratorContext<PeriodicSplit<OUT>> enumContext,
            PeriodicEnumeratorState checkpoint)
            throws Exception {
        return new PeriodicEnumerator<>(
                enumContext,
                checkpoint,
                supplier,
                initialDelayMillis,
                discoverPeriodMillis);
    }

    @Override
    public SimpleVersionedSerializer<PeriodicSplit<OUT>> getSplitSerializer() {
        return new SimpleSerializer<>();
    }

    @Override
    public SimpleVersionedSerializer<PeriodicEnumeratorState> getEnumeratorCheckpointSerializer() {
        return new SimpleSerializer<>();
    }

}
