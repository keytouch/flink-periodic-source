package net.krmo.flink.source.periodic;

import java.io.IOException;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class PeriodicEnumeratorStateSerializer implements SimpleVersionedSerializer<PeriodicEnumeratorState> {

    public static final PeriodicEnumeratorStateSerializer INSTANCE = new PeriodicEnumeratorStateSerializer();

    private PeriodicEnumeratorStateSerializer() {
    }

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(PeriodicEnumeratorState obj) throws IOException {
        // TODO: dummy for now
        return new byte[0];
    }

    @Override
    public PeriodicEnumeratorState deserialize(int version, byte[] serialized) throws IOException {
        // TODO: dummy for now
        return new PeriodicEnumeratorState();
    }

}
