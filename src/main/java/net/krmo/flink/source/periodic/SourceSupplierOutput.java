package net.krmo.flink.source.periodic;

import net.krmo.flink.source.periodic.event.PeriodicConfig;

public class SourceSupplierOutput<OUT> {

    public final String id;
    public final OUT value;
    public final PeriodicConfig periodicConfig;

    public SourceSupplierOutput(String id, OUT value, PeriodicConfig config) {
        this.id = id;
        this.value = value;
        this.periodicConfig = config;
    }
}
