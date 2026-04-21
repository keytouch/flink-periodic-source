package net.krmo.flink.source.periodic.reader;

import java.util.Map;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import net.krmo.flink.source.periodic.PeriodicSplit;

public class PeriodicReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<OUT, OUT, PeriodicSplit, PeriodicSplit> {

    public PeriodicReader(SourceReaderContext context) {
        super(
                () -> new PeriodicSplitReader<>(),
                (element, output, splitState) -> output.collect(element), // pass
                context.getConfiguration(),
                context);
    }

    @Override
    protected void onSplitFinished(Map<String, PeriodicSplit> finishedSplitIds) {
        // never
    }

    @Override
    protected PeriodicSplit initializedState(PeriodicSplit split) {
        return split;
    }

    @Override
    protected PeriodicSplit toSplitType(String splitId, PeriodicSplit splitState) {
        return splitState;
    }

}
