package se.kuseman.payloadbuilder.core.physicalplan;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.ArrayList;
import java.util.List;

import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.Payloadbuilder;
import se.kuseman.payloadbuilder.core.RawQueryResult;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageData;
import se.kuseman.payloadbuilder.core.execution.QueryCoverageRegistry;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator;
import se.kuseman.payloadbuilder.core.execution.vector.VectorFactory;

/** Tests for {@link CoveragePlan}. */
public class CoveragePlanTest
{
    private QuerySession session;
    private final List<QueryCoverageData> coverageResults = new ArrayList<>();
    private final QueryCoverageRegistry.CoverageListener listener = coverageResults::add;

    @BeforeEach
    void setUp()
    {
        session = new QuerySession(new CatalogRegistry());
        session.setVectorFactory(new VectorFactory(new BufferAllocator()));
        QueryCoverageRegistry.register(listener);
    }

    @AfterEach
    void tearDown()
    {
        QueryCoverageRegistry.deregister(listener);
        coverageResults.clear();
    }

    @Test
    public void testCoveragePlanAddsColumn()
    {
        List<TupleVector> vectors = executeRaw("select 1 + 1 as result");
        assertFalse(vectors.isEmpty());

        TupleVector tv = vectors.get(0);
        Schema schema = tv.getSchema();
        int schemaSize = schema.getSize();

        assertTrue(schemaSize >= 2, "Expected at least 2 columns (result + __coverage__)");
        assertEquals(CoverageTupleVector.COVERAGE_COLUMN, schema.getColumns()
                .get(schemaSize - 1)
                .getName());

        String coverageJson = tv.getColumn(schemaSize - 1)
                .getString(0)
                .toString();
        assertNotNull(coverageJson);
        assertTrue(coverageJson.startsWith("{"), "Coverage JSON should be a JSON object");
        assertTrue(coverageJson.contains("\"operators\""), "Coverage JSON should contain operators");
    }

    @Test
    public void testCoverageListenerNotified()
    {
        assertTrue(coverageResults.isEmpty());
        executeRaw("select 1");
        assertEquals(1, coverageResults.size());
    }

    @Test
    public void testNoCoverageWithoutRegistry()
    {
        QueryCoverageRegistry.deregister(listener);

        List<TupleVector> vectors = executeRaw("select 1 + 1 as result");
        assertFalse(vectors.isEmpty());

        TupleVector tv = vectors.get(0);
        int schemaSize = tv.getSchema()
                .getSize();

        assertEquals(1, schemaSize, "Without coverage mode, should have exactly 1 column");
        assertFalse(CoverageTupleVector.COVERAGE_COLUMN.equals(tv.getSchema()
                .getColumns()
                .get(0)
                .getName()));

        QueryCoverageRegistry.register(listener);
    }

    @Test
    public void testCaseExpressionBranchTracking()
    {
        // Use a data-dependent CASE so it cannot be constant-folded at compile time
        executeRaw("select case when x > 0 then 'positive' else 'negative' end as sign from (values (1), (-1)) t(x)");

        assertFalse(coverageResults.isEmpty());
        String json = coverageResults.get(0)
                .toJson();

        assertTrue(json.contains("\"branches\""), "Coverage JSON should contain branch data for CASE expressions");
        assertTrue(json.contains("\"when_hits\""), "Coverage JSON should track WHEN clause hits");
    }

    @Test
    public void testFilterOperatorCovered()
    {
        executeRaw("select * from (values (1), (2), (3)) t(x) where x > 1");

        assertFalse(coverageResults.isEmpty());
        String json = coverageResults.get(0)
                .toJson();
        assertTrue(json.contains("\"Filter\""), "Coverage JSON should show Filter operator");
        assertTrue(json.contains("\"covered\":true"), "Filter operator should be covered");
    }

    @Test
    public void testAndConditionTracking()
    {
        // x=1: false AND short-circuits → false (1 row)
        // x=2: true AND true → true (1 row)
        // x=3: true AND false → false (1 row)
        executeRaw("select * from (values (1), (2), (3)) t(x) where x > 1 AND x < 3");

        assertFalse(coverageResults.isEmpty());
        QueryCoverageData data = coverageResults.get(0);

        assertFalse(data.getConditions()
                .isEmpty(), "Coverage should include AND/OR condition entries");

        String json = data.toJson();
        assertTrue(json.contains("\"conditions\""), "Coverage JSON should contain condition data for AND/OR");
        assertTrue(json.contains("\"true_hits\""), "Coverage JSON should track true hits");
        assertTrue(json.contains("\"false_hits\""), "Coverage JSON should track false hits");

        // Exactly one row matched (x=2), two rows did not
        QueryCoverageData.ConditionCoverage cond = data.getConditions()
                .get(0);
        assertEquals(1, cond.trueHits, "One row should satisfy the AND condition");
        assertEquals(2, cond.falseHits, "Two rows should fail the AND condition");
    }

    @Test
    public void testOrConditionTracking()
    {
        // x=1: true OR (short-circuits) → true (1 row)
        // x=2: false OR false → false (1 row)
        // x=3: false OR true → true (1 row)
        executeRaw("select * from (values (1), (2), (3)) t(x) where x = 1 OR x = 3");

        assertFalse(coverageResults.isEmpty());
        QueryCoverageData.ConditionCoverage cond = coverageResults.get(0)
                .getConditions()
                .get(0);
        assertEquals(2, cond.trueHits);
        assertEquals(1, cond.falseHits);
    }

    /**
     * Mirrors a controller that calls hasMoreResults() once and processes only the first writable result set. INSERT INTO statements must be auto-executed so their coverage fires even though the
     * controller never calls consumeResult() for them.
     */
    @Test
    public void testInsertIntoAutoExecutedAndCovered()
    {
        // Two-statement query: INSERT INTO #temp, then SELECT from it
        // Controller pattern: only processes the first writable (SELECT) result
        String sql = "select * into #temp from (values (1), (2), (3)) t(x);\n" + "select * from #temp where x > 1;";

        List<TupleVector> result = new ArrayList<>();
        RawQueryResult raw = Payloadbuilder.compile(session, sql, "svc-query")
                .executeRaw(session);

        // Simulates: if (!rs.hasMoreResults()) return; — only one call, no loop
        if (raw.hasMoreResults())
        {
            raw.consumeResult(new RawQueryResult.ResultConsumer()
            {
                @Override
                public void schema(Schema schema)
                {
                }

                @Override
                public boolean consume(TupleVector vector)
                {
                    result.add(vector);
                    return true;
                }
            });
        }

        // INSERT INTO auto-executed during hasMoreResults() → coverage fires for both statements
        assertEquals(2, coverageResults.size(), "INSERT INTO must fire coverage even though caller only processes one result set");
        assertEquals("svc-query", coverageResults.get(0)
                .getQueryKey(), "INSERT INTO statement should be keyed as the base name");
        assertEquals("svc-query#1", coverageResults.get(1)
                .getQueryKey(), "SELECT statement should be keyed with #1 suffix");

        // The caller saw exactly one result set (the SELECT)
        assertEquals(1, result.size(), "Caller should receive exactly one result set (the SELECT)");

        // INSERT INTO coverage should have operators
        assertFalse(coverageResults.get(0)
                .getOperators()
                .isEmpty(), "INSERT INTO coverage entry must contain operators");
        // SELECT coverage should have operators
        assertFalse(coverageResults.get(1)
                .getOperators()
                .isEmpty(), "SELECT coverage entry must contain operators");
    }

    @Test
    public void testMultiStatementEachStatementGetsSeparateCoverageEntry()
    {
        // Three-statement named query — each should produce its own accumulator entry
        List<TupleVector> vectors = executeRaw("select * from (values (1), (2)) t(x);\n" + "select * from (values (3)) t(y) where y > 0;\n" + "select * from (values (10), (20), (30)) t(z);",
                "multi-stmt");

        // Three result-sets returned
        assertEquals(3, vectors.size(), "Expected 3 result sets from 3 statements");

        // Three separate coverage entries — one per statement key
        assertEquals(3, coverageResults.size(), "Expected one coverage entry per statement");
        assertEquals("multi-stmt", coverageResults.get(0)
                .getQueryKey());
        assertEquals("multi-stmt#1", coverageResults.get(1)
                .getQueryKey());
        assertEquals("multi-stmt#2", coverageResults.get(2)
                .getQueryKey());

        // Each entry must have at least one operator
        for (QueryCoverageData data : coverageResults)
        {
            assertFalse(data.getOperators()
                    .isEmpty(), "Expected operators in coverage entry for " + data.getQueryKey());
        }
    }

    private List<TupleVector> executeRaw(String query)
    {
        return executeRaw(query, null);
    }

    private List<TupleVector> executeRaw(String query, String queryName)
    {
        List<TupleVector> result = new ArrayList<>();
        RawQueryResult raw = Payloadbuilder.compile(session, query, queryName)
                .executeRaw(session);
        while (raw.hasMoreResults())
        {
            raw.consumeResult(new RawQueryResult.ResultConsumer()
            {
                @Override
                public void schema(Schema schema)
                {
                }

                @Override
                public boolean consume(TupleVector vector)
                {
                    result.add(vector);
                    return true;
                }
            });
        }
        return result;
    }
}
