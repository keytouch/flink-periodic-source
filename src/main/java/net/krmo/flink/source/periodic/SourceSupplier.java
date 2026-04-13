package net.krmo.flink.source.periodic;

import java.io.Serializable;
import java.util.Collection;
import java.util.function.Supplier;

public interface SourceSupplier<OUT> extends Supplier<Collection<SourceSupplierOutput<OUT>>>, Serializable {
    default void open() throws Exception {
    }
}
