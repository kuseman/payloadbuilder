package org.kuse.payloadbuilder.core.test;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kuse.payloadbuilder.core.OutputWriterAdapter;
import org.kuse.payloadbuilder.core.Payloadbuilder;
import org.kuse.payloadbuilder.core.QueryResult;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.test.TestCase.ColumnValue;

import com.fasterxml.jackson.databind.ObjectMapper;

import junit.textui.TestRunner;

/** Harness runner test */
@RunWith(Parameterized.class)
public class TestHarnessRunner
{
    private static final ObjectMapper MAPPER = new ObjectMapper();

    private final TestHarness harness;
    private final TestCase testCase;

    @Parameterized.Parameters(name = "{2}")
    public static List<Object[]> testHarnesses()
    {
        List<String> testFiles = asList(
                "BaseContructs.json",
                "Joins.json",
                "BuiltInFunctions.json",
                "TemporaryTables.json");

        List<TestHarness> harnesses = new ArrayList<>();
        for (String file : testFiles)
        {
            String resource = "/harnessCases/" + file;
            InputStream stream = TestRunner.class.getResourceAsStream(resource);
            if (stream == null)
            {
                Assert.fail("No harness resouce found: " + resource);
            }
            try
            {
                harnesses.add(MAPPER.readValue(stream, TestHarness.class));
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error reading harness file " + file, e);
            }
        }

        return harnesses.stream()
                .flatMap(h -> h.getCases().stream().map(c -> Pair.of(h, c)))
                .map(p -> new Object[] {p.getKey(), p.getValue(), p.getKey().getName() + "#" + p.getValue().getName()})
                .collect(toList());
    }

    public TestHarnessRunner(TestHarness harness, TestCase testCase, @SuppressWarnings("unused") String name)
    {
        this.harness = harness;
        this.testCase = testCase;
    }

    @Test
    public void test()
    {
        CatalogRegistry registry = new CatalogRegistry();
        for (TestCatalog catalog : harness.getCatalogs())
        {
            registry.registerCatalog(catalog.getAlias(), new TCatalog(catalog));
        }

        ResultWriter writer = new ResultWriter();
        QuerySession session = new QuerySession(registry);
        // Set first catalog as default
        session.getCatalogRegistry().setDefaultCatalog(harness.getCatalogs().get(0).getAlias());
        List<List<List<ColumnValue>>> actualResultSets = new ArrayList<>();

        boolean fail = false;
        try
        {
            QueryResult result = Payloadbuilder.query(session, testCase.getQuery());
            while (result.hasMoreResults())
            {
                result.writeResult(writer);
                actualResultSets.add(writer.reset());
            }

            if (testCase.getExpectedException() != null)
            {
                fail = true;
                fail("Expected " + testCase.getExpectedException() + " to be thrown");
            }
        }
        catch (Throwable e)
        {
            if (fail)
            {
                throw e;
            }

            if (testCase.getExpectedException() != null)
            {
                Assert.assertTrue(
                        "Expected " + testCase.getExpectedException() + " to be thrown, but got " + e.getClass(),
                        e.getClass().isAssignableFrom(testCase.getExpectedException()));

                if (!isBlank(testCase.getExpectedMessageContains()))
                {
                    Assert.assertTrue(
                            "Expected message to contain " + testCase.getExpectedMessageContains() + ", but got " + e.getMessage(),
                            containsIgnoreCase(e.getMessage(), testCase.getExpectedMessageContains()));
                }

                return;
            }
            else
            {
                throw e;
            }
        }

        int size = testCase.getExpectedResultSets().size();

        Assert.assertEquals("Expected number of result sets to be equal", size, actualResultSets.size());

        for (int i = 0; i < size; i++)
        {
            Assert.assertEquals("Expected rows in result set: " + (i + 1) + " to be equal", testCase.getExpectedResultSets().get(i), actualResultSets.get(i));
        }
    }

    /** Harness catalog */
    private static class TCatalog extends Catalog
    {
        private final TestCatalog catalog;

        TCatalog(TestCatalog catalog)
        {
            super("Test#" + catalog.getAlias());
            this.catalog = catalog;
        }

        @Override
        public Operator getScanOperator(OperatorData data)
        {
            for (TestTable table : catalog.getTables())
            {
                if (equalsIgnoreCase(table.getName(), data.getTableAlias().getTable().toString()))
                {
                    return new TOperator(table, data.getTableAlias(), data.getNodeId());
                }
            }

            throw new IllegalArgumentException("No test table setup with name: " + data.getTableAlias().getTable());
        }
    }

    /** Harness operator */
    private static class TOperator implements Operator
    {
        private final TestTable table;
        private final TableAlias alias;
        private final int nodeId;

        TOperator(TestTable table, TableAlias alias, int nodeId)
        {
            this.table = table;
            this.alias = alias;
            this.nodeId = nodeId;
        }

        @Override
        public RowIterator open(ExecutionContext context)
        {
            final Iterator<Object[]> it = table.getRows().iterator();
            final String[] columns = table.getColumns().toArray(ArrayUtils.EMPTY_STRING_ARRAY);
            return new RowIterator()
            {
                private int pos;

                @Override
                public Tuple next()
                {
                    return Row.of(alias, pos++, columns, it.next());
                }

                @Override
                public boolean hasNext()
                {
                    return it.hasNext();
                }
            };
        }

        @Override
        public int getNodeId()
        {
            return nodeId;
        }
    }

    /** Harness result writer */
    private static class ResultWriter extends OutputWriterAdapter
    {
        private List<List<ColumnValue>> rows = new ArrayList<>();

        private List<ColumnValue> row;
        private String column;

        List<List<ColumnValue>> reset()
        {
            List<List<ColumnValue>> result = rows;
            rows = new ArrayList<>();
            return result;
        }

        @Override
        public void startRow()
        {
            row = new ArrayList<>();
        }

        @Override
        public void endRow()
        {
            rows.add(row);
        }

        @Override
        public void writeFieldName(String name)
        {
            column = name;
        }

        @SuppressWarnings("unchecked")
        @Override
        public void writeValue(Object value)
        {
            Object result = value;
            if (result instanceof Iterator)
            {
                result = IteratorUtils.toList((Iterator<Object>) result);
            }
            row.add(new ColumnValue(column, result));
        }
    }
}
