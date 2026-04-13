package net.krmo.flink.source.periodic.reader;

import java.util.Map;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;

import net.krmo.flink.source.periodic.PeriodicSplit;

public class PeriodicReader<OUT>
        extends SingleThreadMultiplexSourceReaderBase<OUT, OUT, PeriodicSplit<OUT>, PeriodicSplit<OUT>> {

    public PeriodicReader(SourceReaderContext context) {
        super(
                () -> new PeriodicSplitReader<>(),
                (element, output, splitState) -> output.collect(element), // pass
                context.getConfiguration(),
                context);
    }

    @Override
    protected void onSplitFinished(Map<String, PeriodicSplit<OUT>> finishedSplitIds) {
        // never
    }

    @Override
    protected PeriodicSplit<OUT> initializedState(PeriodicSplit<OUT> split) {
        return split;
    }

    @Override
    protected PeriodicSplit<OUT> toSplitType(String splitId, PeriodicSplit<OUT> splitState) {
        return splitState;
    }

}
