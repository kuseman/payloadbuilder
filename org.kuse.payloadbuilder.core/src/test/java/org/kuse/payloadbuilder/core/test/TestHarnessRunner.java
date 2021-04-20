package org.kuse.payloadbuilder.core.test;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.containsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;
import org.kuse.payloadbuilder.core.CompiledQuery;
import org.kuse.payloadbuilder.core.Payloadbuilder;
import org.kuse.payloadbuilder.core.QueryResult;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.operator.AObjectOutputWriter;
import org.kuse.payloadbuilder.core.operator.AObjectOutputWriter.ColumnValue;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.utils.ExpressionMath;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import junit.textui.TestRunner;

/** Harness runner test */
@RunWith(Parameterized.class)
public class TestHarnessRunner
{
    private static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

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
        Assume.assumeFalse("Ignored", testCase.isIgnore());
        testInternal(false);
        testInternal(true);
    }

    //CSOFF
    private void testInternal(boolean codeGen)
    //CSON
    {
        CatalogRegistry registry = new CatalogRegistry();
        for (TestCatalog catalog : harness.getCatalogs())
        {
            registry.registerCatalog(catalog.getAlias(), new TCatalog(catalog));
        }

        List<List<List<ColumnValue>>> actualResultSets = new ArrayList<>();
        AObjectOutputWriter writer = new AObjectOutputWriter()
        {
            @Override
            protected void consumeRow(List<ColumnValue> row)
            {
                actualResultSets.get(actualResultSets.size() - 1).add(row);
            }
        };

        StringWriter sw = new StringWriter();
        PrintWriter pw = new PrintWriter(sw)
        {
            @Override
            public void flush()
            {
                throw new RuntimeException("Error:" + sw.toString());
            }
        };

        QuerySession session = new QuerySession(registry);
        session.setPrintWriter(pw);

        if (codeGen)
        {
            session.setSystemProperty(QuerySession.CODEGEN_ENABLED, true);
        }

        // Set first catalog as default
        session.getCatalogRegistry().setDefaultCatalog(harness.getCatalogs().get(0).getAlias());

        boolean fail = false;
        try
        {
            CompiledQuery query = Payloadbuilder.compile(testCase.getQuery(), session.getCatalogRegistry());
            QueryResult result = query.execute(session);
            while (result.hasMoreResults())
            {
                actualResultSets.add(new ArrayList<>());
                result.writeResult(writer);
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

        Assert.assertEquals((codeGen ? "CodeGen: " : "") + "Expected number of result sets to be equal", size, actualResultSets.size());

        for (int i = 0; i < size; i++)
        {
            assertResultSetEqual(codeGen, i, testCase.getExpectedResultSets().get(i), actualResultSets.get(i));
        }
    }

    private void assertResultSetEqual(boolean codeGen, int number, List<List<ColumnValue>> expected, List<List<ColumnValue>> actual)
    {
        int size = expected.size();
        if (size != actual.size())
        {
            fail((codeGen ? "CodeGen: " : "") + "Result set number: " + (number + 1) + ", expected size " + size + " but was " + actual.size());
        }

        for (int i = 0; i < size; i++)
        {
            List<ColumnValue> expectedRow = expected.get(i);
            List<ColumnValue> actualRow = actual.get(i);

            int rowSize = expectedRow.size();
            if (rowSize != actualRow.size())
            {
                fail((codeGen ? "CodeGen: " : "") + "Result set number: " + (number + 1) + ", row number: " + i + ", expected size " + rowSize + " but was " + actualRow.size());
            }

            for (int j = 0; j < rowSize; j++)
            {
                ColumnValue expectedColumn = expectedRow.get(j);
                ColumnValue actualColumn = actualRow.get(j);

                if (!equals(expectedColumn, actualColumn))
                {
                    fail((codeGen ? "CodeGen: " : "") + "Result set number: " + (number + 1) + ", row: " + i + ", col: " + j + ", expected " + expectedColumn + " but was " + actualColumn + System.lineSeparator() + "Expected: " + System.lineSeparator() + expected.stream().map(Object::toString).collect(joining(System.lineSeparator())) + System.lineSeparator() + ", but was:" + System.lineSeparator() + actual.stream().map(Object::toString).collect(joining(System.lineSeparator())));
                }
            }
        }
    }

    private boolean equals(ColumnValue expected, ColumnValue actual)
    {
        if (!Objects.equals(expected.getKey(), actual.getKey()))
        {
            return false;
        }

        if (expected.getValue() == null)
        {
            return actual.getValue() == null;
        }
        else if (actual.getValue() == null)
        {
            return false;
        }

        // Use expression math to handle float = double etc.
        if (ExpressionMath.eq(expected.getValue(), actual.getValue(), false))
        {
            return true;
        }

        return Objects.equals(expected.getValue(), actual.getValue());
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
}
