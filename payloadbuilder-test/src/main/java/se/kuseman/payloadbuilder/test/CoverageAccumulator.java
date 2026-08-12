package se.kuseman.payloadbuilder.test;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.core.execution.QueryCoverageData;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageRegistry;

/**
 * Accumulates {@link QueryCoverageData} from all queries executed during a test class run. Multiple executions of the same query (same plan structure or query name) are merged into a single entry
 * rather than recorded separately. Thread-safe: the listener may be called from multiple test threads when JUnit parallel execution is enabled.
 */
public class CoverageAccumulator implements QueryCoverageRegistry.CoverageListener
{
    private final String testClassName;
    /** Insertion-ordered map keyed by query key (compile-time name or plan-structure hash). */
    private final Map<String, QueryCoverageData> byKey = new LinkedHashMap<>();

    /** Creates an accumulator for the given test class display name. */
    public CoverageAccumulator(String testClassName)
    {
        this.testClassName = testClassName;
    }

    @Override
    public synchronized void onQueryCoverage(QueryCoverageData data)
    {
        QueryCoverageData existing = byKey.get(data.getQueryKey());
        if (existing == null)
        {
            byKey.put(data.getQueryKey(), data);
        }
        else
        {
            existing.merge(data);
        }
    }

    /** Returns the test class name this accumulator belongs to. */
    public String getTestClassName()
    {
        return testClassName;
    }

    /** Returns all aggregated coverage entries — one per unique query. */
    public synchronized List<QueryCoverageData> getCollected()
    {
        return new ArrayList<>(byKey.values());
    }
}
