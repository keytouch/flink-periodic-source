package net.krmo.flink.examples;

import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.flink.api.common.eventtime.WatermarkStrategy;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import net.krmo.flink.source.periodic.PeriodicSource;
import net.krmo.flink.source.periodic.SourceSupplierOutput;
import net.krmo.flink.source.periodic.event.PeriodicConfig;

/**
 * An example on the usage of PeriodicSource
 */
public class PeriodicSourceExample {

    public static void main(String[] args) throws Exception {
        final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
        env.setParallelism(2);

        env.fromSource(new PeriodicSource<>(
                () -> {
                    List<SourceSupplierOutput<String>> a = new ArrayList<>();
                    a.add(new SourceSupplierOutput<>("1", "1s AAA",
                            new PeriodicConfig(true, 0, 1, TimeUnit.SECONDS)));
                    a.add(new SourceSupplierOutput<>("2", "5s BBB",
                            new PeriodicConfig(true, 0, 5, TimeUnit.SECONDS)));
                    a.add(new SourceSupplierOutput<>("3", "15s CCC",
                            new PeriodicConfig(true, 0, 15, TimeUnit.SECONDS)));
                    return a;
                }, 0, 10000),
                WatermarkStrategy.noWatermarks(),
                "Periodic-Source",
                TypeInformation.of(String.class))
                .map(v -> Instant.now() + ": " + v)
                .print();

        env.execute();
    }
}
