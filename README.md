# Flink Periodic Source

A Flink Connector (Source ONLY, Data Source API) with periodic events emitting.

### Suitable for:

* Testing with periodic emitting events
* When a time driven event source is needed

### Usage

1. Create the Source
    ```java
    new PeriodicSource<>(SourceSupplier, initialDelayMillis, discoverPeriodMillis);
    ```

2. `SourceSupplier` should return a `Collection` of `SourceSupplierOutput`

    Every `SourceSupplierOutput` consists of an `id` (for pretty logging when assigned), `value` and `PeriodicConfig`
    ```java
    new SourceSupplierOutput<>(String id, OUT value, PeriodicConfig config);
    ```
    **NOTE:** **DO NOT** try to schedule tons of events, event flow should be redesigned properly in this case

    `PeriodicConfig`:
    ```java
    new PeriodicConfig(boolean useWallTime, long initialDelay, long period, TimeUnit unit);
    ```
    - Wall time and fixed rate scheduling are supported, `useWallTime` = `true` indicates wall time scheduling.
    - `initialDelay` represents the trigger delay in the period in wall time mode. With 10s period, 1s means at xx:01, xx:11, xx:21...
    - `period` represents the schedule interval in wall time mode, 10s means at xx:00, xx:10, xx:20...
    - `unit` is the unit of initialDelay and period

3. `SourceSupplier` is invoked every `discoverPeriodMillis` ms, with an initial delay of `initialDelayMillis` ms after all subtasks are online.

   - `SourceSupplier` runs on jobmanager.
   - `discoverPeriodMillis` should not be too small. Too frequent assigning may interrupt the scheduling.

4. Events returned by `SourceSupplier` will be assigned to subtasks in a round-robin fashion.

5. Then every subtask emits the events based on the given `PeriodicConfig`.

6. If any Exception is thrown from `SourceSupplier`, subtasks continue to emit events with previously successfully assigned events.

### Example

Check [here](src/test/java/net/krmo/flink/examples) for more
```java
final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

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
        }, 0, 60000),
        WatermarkStrategy.noWatermarks(),
        "Periodic-Source",
        TypeInformation.of(String.class))
        .map(v -> Instant.now() + ": " + v)
        .print();

env.execute();
```
