package net.krmo.flink.source;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

import org.apache.flink.core.io.SimpleVersionedSerializer;

public class SimpleSerializer<T> implements SimpleVersionedSerializer<T> {

    @Override
    public int getVersion() {
        return 0;
    }

    @Override
    public byte[] serialize(T obj) throws IOException {
        try (
                ByteArrayOutputStream baos = new ByteArrayOutputStream();
                ObjectOutputStream oos = new ObjectOutputStream(baos)) {
            oos.writeObject(obj);
            return baos.toByteArray();
        }
    }

    @SuppressWarnings("unchecked")
    @Override
    public T deserialize(int version, byte[] serialized) throws IOException {
        try (
                ByteArrayInputStream bais = new ByteArrayInputStream(serialized);
                ObjectInputStream ois = new ObjectInputStream(bais)) {
            return (T) ois.readObject();
        } catch (ClassNotFoundException e) {
            throw new IOException(e);
        }
    }

}
