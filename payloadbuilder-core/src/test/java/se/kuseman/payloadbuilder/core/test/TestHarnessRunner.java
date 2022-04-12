package se.kuseman.payloadbuilder.core.test;

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

import junit.textui.TestRunner;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.api.utils.ExpressionMath;
import se.kuseman.payloadbuilder.core.CompiledQuery;
import se.kuseman.payloadbuilder.core.Payloadbuilder;
import se.kuseman.payloadbuilder.core.QueryResult;
import se.kuseman.payloadbuilder.core.QuerySession;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.operator.AObjectOutputWriter;
import se.kuseman.payloadbuilder.core.operator.AObjectOutputWriter.ColumnValue;

/** Harness runner test */
@RunWith(Parameterized.class)
public class TestHarnessRunner
{
    private static final ObjectMapper MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final TestHarness harness;
    private final TestCase testCase;

    /** Test */
    @Parameterized.Parameters(
            name = "{2}")
    public static List<Object[]> testHarnesses()
    {
        List<String> testFiles = asList("BaseContructs.json", "Joins.json", "SystemFunctions.json", "TemporaryTables.json");

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
                .flatMap(h -> h.getCases()
                        .stream()
                        .map(c -> Pair.of(h, c)))
                .map(p -> new Object[] {
                        p.getKey(), p.getValue(), p.getKey()
                                .getName() + "#"
                                                  + p.getValue()
                                                          .getName() })
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

    // CSOFF
    private void testInternal(boolean codeGen)
    // CSON
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
                actualResultSets.get(actualResultSets.size() - 1)
                        .add(row);
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
        session.setDefaultCatalogAlias(harness.getCatalogs()
                .get(0)
                .getAlias());

        boolean fail = false;
        try
        {
            CompiledQuery query = Payloadbuilder.compile(testCase.getQuery());
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
                Assert.assertTrue("Expected " + testCase.getExpectedException() + " to be thrown, but got " + e.getClass(), e.getClass()
                        .isAssignableFrom(testCase.getExpectedException()));

                if (!isBlank(testCase.getExpectedMessageContains()))
                {
                    Assert.assertTrue("Expected message to contain " + testCase.getExpectedMessageContains() + ", but got " + e.getMessage(),
                            containsIgnoreCase(e.getMessage(), testCase.getExpectedMessageContains()));
                }

                return;
            }
            else
            {
                throw e;
            }
        }

        int size = testCase.getExpectedResultSets()
                .size();

        Assert.assertEquals((codeGen ? "CodeGen: "
                : "") + "Expected number of result sets to be equal", size, actualResultSets.size());

        for (int i = 0; i < size; i++)
        {
            assertResultSetEqual(codeGen, i, testCase.getExpectedResultSets()
                    .get(i), actualResultSets.get(i));
        }
    }

    private void assertResultSetEqual(boolean codeGen, int number, List<List<ColumnValue>> expected, List<List<ColumnValue>> actual)
    {
        int size = expected.size();
        if (size != actual.size())
        {
            fail((codeGen ? "CodeGen: "
                    : "")
                 + "Result set number: "
                 + (number + 1)
                 + ", expected size "
                 + size
                 + " but was "
                 + actual.size());
        }

        for (int i = 0; i < size; i++)
        {
            List<ColumnValue> expectedRow = expected.get(i);
            List<ColumnValue> actualRow = actual.get(i);

            int rowSize = expectedRow.size();
            if (rowSize != actualRow.size())
            {
                fail((codeGen ? "CodeGen: "
                        : "")
                     + "Result set number: "
                     + (number + 1)
                     + ", row number: "
                     + i
                     + ", expected size "
                     + rowSize
                     + " but was "
                     + actualRow.size());
            }

            for (int j = 0; j < rowSize; j++)
            {
                ColumnValue expectedColumn = expectedRow.get(j);
                ColumnValue actualColumn = actualRow.get(j);

                if (!equals(expectedColumn, actualColumn))
                {
                    fail((codeGen ? "CodeGen: "
                            : "")
                         + "Result set number: "
                         + (number + 1)
                         + ", row: "
                         + i
                         + ", col: "
                         + j
                         + ", expected "
                         + expectedColumn
                         + " but was "
                         + actualColumn
                         + System.lineSeparator()
                         + "Expected: "
                         + System.lineSeparator()
                         + expected.stream()
                                 .map(Object::toString)
                                 .collect(joining(System.lineSeparator()))
                         + System.lineSeparator()
                         + ", but was:"
                         + System.lineSeparator()
                         + actual.stream()
                                 .map(Object::toString)
                                 .collect(joining(System.lineSeparator())));
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

        // toString-ify the actual if we expect a string
        if (expected.getValue() instanceof String
                && !(actual.getValue() instanceof String))
        {
            return Objects.equals(expected.getValue(), String.valueOf(actual.getValue()));
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
        public Operator getSystemOperator(OperatorData data)
        {
            TableAlias alias = data.getTableAlias();
            String type = alias.getTable()
                    .getLast();

            if (SYS_TABLES.equalsIgnoreCase(type))
            {
                return ctx -> getTables(alias);
            }
            else if (SYS_COLUMNS.equalsIgnoreCase(type))
            {
                return ctx -> getColumns(alias);
            }
            else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
            {
                return getFunctionsOperator(data.getNodeId(), alias);
            }
            else if (SYS_INDICES.equalsIgnoreCase(type))
            {
                return Operator.EMPTY_OPERATOR;
            }

            return super.getSystemOperator(data);
        }

        @Override
        public Operator getScanOperator(OperatorData data)
        {
            String operatorTable = data.getTableAlias()
                    .getTable()
                    .getLast();
            for (TestTable table : catalog.getTables())
            {
                if (equalsIgnoreCase(table.getName(), operatorTable))
                {
                    return new TOperator(table, data.getTableAlias(), data.getNodeId());
                }
            }

            throw new IllegalArgumentException("No test table setup with name: " + data.getTableAlias()
                    .getTable());
        }

        private TupleIterator getTables(TableAlias tableAlias)
        {
            String[] columns = new String[] { SYS_TABLES_NAME, "columns" };
            return TupleIterator.wrap(catalog.getTables()
                    .stream()
                    .map(t -> (Tuple) Row.of(tableAlias, columns, new Object[] { t.getName(), t.getColumns() }))
                    .iterator());
        }

        private TupleIterator getColumns(TableAlias tableAlias)
        {
            String[] columns = new String[] { SYS_COLUMNS_TABLE, SYS_COLUMNS_NAME, "custom" };
            return TupleIterator.wrap(catalog.getTables()
                    .stream()
                    .flatMap(t -> t.getColumns()
                            .stream()
                            .map(c -> Pair.of(t, c)))
                    .map(p -> (Tuple) Row.of(tableAlias, columns, new Object[] {
                            p.getKey()
                                    .getName(),
                            p.getValue(), p.getValue()
                                    .length() }))
                    .iterator());
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
        public TupleIterator open(IExecutionContext context)
        {
            final Iterator<Object[]> it = table.getRows()
                    .iterator();
            final String[] columns = table.getColumns()
                    .toArray(ArrayUtils.EMPTY_STRING_ARRAY);
            return new TupleIterator()
            {
                @Override
                public Tuple next()
                {
                    return Row.of(alias, columns, it.next());
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
