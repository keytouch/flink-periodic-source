package net.krmo.flink.source.periodic.reader;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.DelayQueue;
import java.util.concurrent.TimeUnit;

import org.apache.flink.connector.base.source.reader.RecordsBySplits;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.splitreader.SplitsChange;

import net.krmo.flink.source.periodic.PeriodicSplit;
import net.krmo.flink.source.periodic.event.Event;

public class PeriodicSplitReader<OUT> implements SplitReader<OUT, PeriodicSplit<OUT>> {

    private final DelayQueue<Event<OUT>> eventQueue = new DelayQueue<>();

    private PeriodicSplit<OUT> currentSplit;
    private PeriodicSplit<OUT> pendingSplit;

    @Override
    public void close() throws Exception {
    }

    @Override
    public RecordsWithSplitIds<OUT> fetch() throws IOException {
        Map<String, Collection<OUT>> recordsBySplit = new HashMap<>();
        Set<String> finishedSplits = new HashSet<>(); // TODO: when to finish splits? never?

        // poll and wait for 1s max
        Event<OUT> poll;
        try {
            poll = eventQueue.poll(1, TimeUnit.SECONDS);
        } catch (InterruptedException e) {
            return new RecordsBySplits<>(recordsBySplit, finishedSplits);
        }
        if (poll == null) {
            return new RecordsBySplits<>(recordsBySplit, finishedSplits);
        }
        poll.setNextRunTime();
        eventQueue.add(poll);
        recordsBySplit.computeIfAbsent(currentSplit.splitId(), k -> new ArrayList<>()).add(poll.getValue());

        // drain
        List<Event<OUT>> pollList = new ArrayList<>();
        eventQueue.drainTo(pollList);
        for (Event<OUT> event : pollList) {
            event.setNextRunTime();
            eventQueue.add(event);
            recordsBySplit.computeIfAbsent(currentSplit.splitId(), k -> new ArrayList<>()).add(event.getValue());
        }

        return new RecordsBySplits<>(recordsBySplit, finishedSplits);
    }

    @Override
    public void handleSplitsChanges(SplitsChange<PeriodicSplit<OUT>> splitsChanges) {
        // only one split per subtask
        pendingSplit = splitsChanges.splits().get(0);
        processPendingSplit();
    }

    @Override
    public void wakeUp() {
        // fetch will block for 1s max
    }

    /**
     * 1. schedule and add new events into the queue.
     * existing events are not touched.
     * 2. remove events no longer existing.
     */
    private void processPendingSplit() {
        if (pendingSplit != null) {
            currentSplit = pendingSplit;
            pendingSplit = null;

            List<Event<OUT>> events = currentSplit.getEvents();
            for (Event<OUT> event : events) {
                if (eventQueue.contains(event)) {
                    continue;
                }
                event.setNextRunTime();
                eventQueue.add(event);
            }
            eventQueue.removeIf(e -> !events.contains(e));
        }
    }

}
