package net.krmo.flink.source.periodic.event;

import java.io.Serializable;
import java.util.concurrent.Delayed;
import java.util.concurrent.TimeUnit;

public class Event<T> implements Delayed, Serializable {

    private final String id;
    private final T value;
    private final PeriodicConfig periodicConfig;

    /** next timestamp to run in millis */
    private transient long time;

    public Event(String id, T value, PeriodicConfig periodicConfig) {
        this.id = id;
        this.value = value;
        this.periodicConfig = periodicConfig;
    }

    public String getId() {
        return id;
    }

    public T getValue() {
        return value;
    }

    public PeriodicConfig getPeriodicConfig() {
        return periodicConfig;
    }

    public void setNextRunTime() {
        TimeUnit unit = periodicConfig.unit;
        long delayMillis = unit.toMillis(periodicConfig.initialDelay);
        long periodMillis = unit.toMillis(periodicConfig.period);

        if (periodicConfig.useWallTime) {
            // always skip late runs
            time = System.currentTimeMillis();
            time += periodMillis - (time - delayMillis) % periodMillis;
        } else {
            if (time == 0) {
                time = System.currentTimeMillis() + delayMillis;
            } else {
                time += periodMillis;
            }
        }
    }

    public long getNextRunTime() {
        return time;
    }

    @Override
    public long getDelay(TimeUnit unit) {
        return unit.convert(time - System.currentTimeMillis(), TimeUnit.MILLISECONDS);
    }

    @Override
    public int compareTo(Delayed o) {
        if (o == this) // compare zero if same object
            return 0;
        long diff = getDelay(TimeUnit.MILLISECONDS) - o.getDelay(TimeUnit.MILLISECONDS);
        return (diff < 0) ? -1 : (diff > 0) ? 1 : 0;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
        result = prime * result + ((value == null) ? 0 : value.hashCode());
        result = prime * result + ((periodicConfig == null) ? 0 : periodicConfig.hashCode());
        return result;
    }

    @Override
    public boolean equals(Object obj) {
        if (this == obj)
            return true;
        if (obj == null)
            return false;
        if (getClass() != obj.getClass())
            return false;
        Event<?> other = (Event<?>) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        if (value == null) {
            if (other.value != null)
                return false;
        } else if (!value.equals(other.value))
            return false;
        if (periodicConfig == null) {
            if (other.periodicConfig != null)
                return false;
        } else if (!periodicConfig.equals(other.periodicConfig))
            return false;
        return true;
    }

}
