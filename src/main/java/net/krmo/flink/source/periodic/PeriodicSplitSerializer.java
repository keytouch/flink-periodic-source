package net.krmo.flink.source.periodic;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.io.Serializable;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.core.io.SimpleVersionedSerializer;
import net.krmo.flink.source.periodic.event.Event;
import net.krmo.flink.source.periodic.event.PeriodicConfig;

public class PeriodicSplitSerializer implements SimpleVersionedSerializer<PeriodicSplit> {

    public static final PeriodicSplitSerializer INSTANCE = new PeriodicSplitSerializer();

    private PeriodicSplitSerializer() {
    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(PeriodicSplit obj) throws IOException {
        try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream out = new ObjectOutputStream(baos)) {
            out.writeUTF(obj.splitId());
            List<Event<Serializable>> events = obj.getEvents();
            out.writeInt(events.size());
            for (Event<Serializable> event : events) {
                out.writeUTF(event.getId());
                out.writeBoolean(event.getPeriodicConfig().useWallTime);
                out.writeLong(event.getPeriodicConfig().initialDelay);
                out.writeLong(event.getPeriodicConfig().period);
                out.writeObject(event.getPeriodicConfig().unit);
                // value is user provided
                out.writeObject(event.getValue());
            }
            out.flush();
            return baos.toByteArray();
        }
    }

    @Override
    public PeriodicSplit deserialize(int version, byte[] serialized) throws IOException {
        try (ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                ObjectInputStream in = new ObjectInputStream(bais)) {
            String splitId = in.readUTF();
            PeriodicSplit split = new PeriodicSplit(splitId);
            int eventsSize = in.readInt();
            for (int i = 0; i < eventsSize; i++) {
                String id = in.readUTF();
                boolean useWallTime = in.readBoolean();
                long initialDelay = in.readLong();
                long period = in.readLong();
                TimeUnit unit = (TimeUnit) in.readObject();
                Serializable v = (Serializable) in.readObject();
                split.addEvent(new Event<>(id, v, new PeriodicConfig(useWallTime, initialDelay, period, unit)));
            }
            return split;
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

}
