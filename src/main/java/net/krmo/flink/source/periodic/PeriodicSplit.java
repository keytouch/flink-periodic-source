package net.krmo.flink.source.periodic;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.List;
import org.apache.flink.api.connector.source.SourceSplit;

import net.krmo.flink.source.periodic.event.Event;

/**
 * Since our split is just a bunch of events,
 * We assign ONLY one split to each subtask per assignment.
 * One split contains a list of events that need to be schedule by the reader.
 * 
 * On arrival of split assignment, reader checks the current event queue against
 * the events in the coming split, and decides which events need to be added or
 * removed. Existing events will not be touched, so as to keep its scheduling
 * states (due events may be missed if we try to requeue all events)
 */
public class PeriodicSplit implements SourceSplit, Serializable {

    private final String id;

    private final List<Event<Serializable>> events = new ArrayList<>();

    public PeriodicSplit(String id) {
        this.id = id;
    }

    @Override
    public String splitId() {
        return id;
    }

    public List<Event<Serializable>> getEvents() {
        return events;
    }

    public void addEvent(Event<Serializable> event) {
        events.add(event);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PeriodicSplit [id=").append(id)
                .append(", events.size=").append(events.size())
                .append(", events=[");
        for (int i = 0; i < events.size(); i++) {
            if (i != 0) {
                sb.append(",");
            }
            sb.append(events.get(i).getId());
        }
        sb.append("]]");
        return sb.toString();
    }

}
