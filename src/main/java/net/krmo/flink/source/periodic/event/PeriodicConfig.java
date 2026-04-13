package net.krmo.flink.source.periodic.event;

import java.io.Serializable;
import java.util.concurrent.TimeUnit;

public class PeriodicConfig implements Serializable {

    /** use wall time instead of periodic scheduling */
    public final boolean useWallTime;
    /** the unit of initialDelay and period */
    public final TimeUnit unit;
    /**
     * in wall time mode, represents the trigger delay in the period
     * with 10s period, 1s means at xx:01, xx:11, xx:21...
     */
    public final long initialDelay;
    /**
     * in wall time mode, represents the schedule interval,
     * 10s means at xx:00, xx:10, xx:20...
     */
    public final long period;

    public PeriodicConfig(boolean useWallTime, long initialDelay, long period, TimeUnit unit) {
        this.useWallTime = useWallTime;
        this.initialDelay = initialDelay;
        this.period = period;
        this.unit = unit;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + (useWallTime ? 1231 : 1237);
        result = prime * result + ((unit == null) ? 0 : unit.hashCode());
        result = prime * result + (int) (initialDelay ^ (initialDelay >>> 32));
        result = prime * result + (int) (period ^ (period >>> 32));
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
        PeriodicConfig other = (PeriodicConfig) obj;
        if (useWallTime != other.useWallTime)
            return false;
        if (unit != other.unit)
            return false;
        if (initialDelay != other.initialDelay)
            return false;
        if (period != other.period)
            return false;
        return true;
    }

}
