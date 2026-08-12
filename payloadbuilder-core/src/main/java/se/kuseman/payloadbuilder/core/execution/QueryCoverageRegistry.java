package se.kuseman.payloadbuilder.core.execution;

import java.util.ArrayList;
import java.util.List;

/**
 * Thread-local registry that connects the coverage-aware query execution engine to external listeners (e.g. JUnit extensions). Listeners are registered per thread so parallel test execution works
 * without interference.
 */
public class QueryCoverageRegistry
{
    private static final ThreadLocal<List<CoverageListener>> LISTENERS = ThreadLocal.withInitial(ArrayList::new);

    private QueryCoverageRegistry()
    {
    }

    /** Registers a listener that will receive coverage data after each query execution on this thread. */
    public static void register(CoverageListener listener)
    {
        LISTENERS.get()
                .add(listener);
    }

    /** Deregisters a previously registered listener. */
    public static void deregister(CoverageListener listener)
    {
        LISTENERS.get()
                .remove(listener);
    }

    /** Returns {@code true} if at least one listener is registered on the current thread. */
    public static boolean isEnabled()
    {
        return !LISTENERS.get()
                .isEmpty();
    }

    /** Notify all registered listeners on the current thread with the given coverage data. */
    public static void notifyListeners(QueryCoverageData data)
    {
        for (CoverageListener listener : LISTENERS.get())
        {
            listener.onQueryCoverage(data);
        }
    }

    /** Listener notified after each query completes with coverage data. */
    public interface CoverageListener
    {
        void onQueryCoverage(QueryCoverageData data);
    }
}
