package se.kuseman.payloadbuilder.api.utils;

/** Simple stop watch */
public class StopWatch
{
    private final long nanoSecondsPerMillisecond = 1000000;
    private final long nanoSecondsPerSecond = 1000000000;
    private final long nanoSecondsPerMinute = 60000000000L;
    private final long nanoSecondsPerHour = 3600000000000L;

    private long stopWatchStartTime = 0;
    private long stopWatchStopTime = 0;
    private boolean stopWatchRunning = false;
    private long totalElapsedTime = 0;

    /** Start the watch */
    public void start()
    {
        if (stopWatchRunning)
        {
            return;
        }

        this.stopWatchStartTime = System.nanoTime();
        this.stopWatchRunning = true;
    }

    /** Stop the watch */
    public void stop()
    {
        if (!stopWatchRunning)
        {
            return;
        }

        this.stopWatchStopTime = System.nanoTime();
        this.stopWatchRunning = false;
        this.totalElapsedTime += (stopWatchStopTime - stopWatchStartTime);
    }

    /** Get elapsed milliseconds */
    public long getElapsedMilliseconds()
    {
        return getElapsedNanos() / nanoSecondsPerMillisecond;
    }

    /** Get elapsed seconds */
    public long getElapsedSeconds()
    {
        return getElapsedNanos() / nanoSecondsPerSecond;
    }

    /** Get elapsed minutes */
    public long getElapsedMinutes()
    {
        return getElapsedNanos() / nanoSecondsPerMinute;
    }

    /** Get elapsed hours */
    public long getElapsedHours()
    {
        return getElapsedNanos() / nanoSecondsPerHour;
    }

    private long getElapsedNanos()
    {
        if (stopWatchRunning)
        {
            return totalElapsedTime + (System.nanoTime() - stopWatchStartTime);
        }
        else
        {
            return totalElapsedTime;
        }
    }
}