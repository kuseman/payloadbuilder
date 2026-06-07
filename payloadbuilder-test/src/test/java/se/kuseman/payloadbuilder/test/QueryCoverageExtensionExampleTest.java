package se.kuseman.payloadbuilder.test;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNotNull;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Stream;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DynamicNode;
import org.junit.jupiter.api.MethodOrderer;
import org.junit.jupiter.api.Order;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestFactory;
import org.junit.jupiter.api.TestMethodOrder;
import org.junit.jupiter.api.extension.RegisterExtension;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.CompiledQuery;
import se.kuseman.payloadbuilder.core.Payloadbuilder;
import se.kuseman.payloadbuilder.core.RawQueryResult;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator;
import se.kuseman.payloadbuilder.core.execution.vector.VectorFactory;
import se.kuseman.payloadbuilder.core.physicalplan.CoveragePlan;

/**
 * End-to-end example for {@link QueryCoverageExtension} and {@link CoverageTestFactory}. Shows named queries (compile-time name via {@code Payloadbuilder.compile(session, sql, queryName)}) and the
 * {@code @TestFactory} at the bottom that produces one JUnit entry per operator and CASE branch once all regular tests have run.
 */
@TestMethodOrder(MethodOrderer.OrderAnnotation.class)
class QueryCoverageExtensionExampleTest
{
    @RegisterExtension
    static final QueryCoverageExtension coverage = new QueryCoverageExtension().registerSource("filter-example", "src/test/resources/queries/filter-example.sql");

    private QuerySession session;

    @BeforeEach
    void setUp()
    {
        session = new QuerySession(new CatalogRegistry());
        session.setVectorFactory(new VectorFactory(new BufferAllocator()));
    }

    @Test
    @Order(1)
    void filterQuery() throws Exception
    {
        // Named at compile time — aggregates across repeated executions and labels coverage nodes
        // with the name "filter-example" instead of a plan hash.
        String sql = readResource("/queries/filter-example.sql");
        CompiledQuery query = Payloadbuilder.compile(session, sql, "filter-example");
        List<TupleVector> rows = execute(query);

        assertEquals(1, rows.size());
        assertEquals(3, rows.get(0)
                .getRowCount());
    }

    @Test
    @Order(2)
    void caseExpression()
    {
        // Unnamed query — falls back to plan-hash container label
        List<TupleVector> rows = execute(
                Payloadbuilder.compile(session, "select case when x > 0 then 'positive' when x < 0 then 'negative' else 'zero' end as sign" + " from (values (1), (-2), (0)) t(x)"));

        assertEquals(1, rows.size());
        int signCol = rows.get(0)
                .getSchema()
                .getSize() - 2;
        assertEquals("positive", rows.get(0)
                .getColumn(signCol)
                .getString(0)
                .toString());
    }

    @Test
    @Order(3)
    void coverageColumnPresent()
    {
        List<TupleVector> rows = execute(Payloadbuilder.compile(session, "select 1 + 1 as result"));

        assertNotNull(rows);
        TupleVector tv = rows.get(0);
        String lastCol = tv.getSchema()
                .getColumns()
                .get(tv.getSchema()
                        .getSize() - 1)
                .getName();
        assertEquals(CoveragePlan.COVERAGE_COLUMN, lastCol);
        String json = tv.getColumn(tv.getSchema()
                .getSize() - 1)
                .getString(0)
                .toString();
        assertTrue(json.contains("\"operators\""));
    }

    // Must be last — accumulator is empty until @Test methods above have run
    @TestFactory
    @Order(Integer.MAX_VALUE)
    Stream<DynamicNode> queryCoverageResults()
    {
        return CoverageTestFactory.from(coverage.getAccumulator());
    }

    // --- helpers ---

    /** Reads a classpath resource (e.g. {@code "/queries/filter-example.sql"}) as a UTF-8 string. */
    private static String readResource(String path) throws Exception
    {
        try (InputStream in = QueryCoverageExtensionExampleTest.class.getResourceAsStream(path))
        {
            if (in == null)
            {
                throw new IllegalArgumentException("Classpath resource not found: " + path);
            }
            return new String(in.readAllBytes(), StandardCharsets.UTF_8).trim();
        }
    }

    private List<TupleVector> execute(CompiledQuery query)
    {
        List<TupleVector> result = new ArrayList<>();
        RawQueryResult raw = query.executeRaw(session);
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
