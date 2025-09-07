package se.kuseman.payloadbuilder.core.test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.Strings.CI;
import static org.junit.Assert.fail;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.TimeZone;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;
import org.junit.Assume;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.junit.runners.Parameterized;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.ProjectionType;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.FunctionType;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.CompiledQuery;
import se.kuseman.payloadbuilder.core.Payloadbuilder;
import se.kuseman.payloadbuilder.core.QueryResult;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.ExpressionMath;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator;
import se.kuseman.payloadbuilder.core.execution.vector.VectorFactory;
import se.kuseman.payloadbuilder.core.test.AObjectOutputWriter.ColumnValue;

/** Harness runner test */
@RunWith(Parameterized.class)
public class TestHarnessRunner
{
    private static final ObjectMapper MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private final TestHarness harness;
    private final TestCase testCase;
    private final boolean schemaLess;

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
            InputStream stream = TestHarnessRunner.class.getResourceAsStream(resource);
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

        List<Object[]> params = new ArrayList<>();

        for (TestHarness harness : harnesses)
        {
            for (TestCase testCase : harness.getCases())
            {
                for (Boolean schemaLess : asList(true, false))
                {
                    if (testCase.getSchemaLess() == null
                            || testCase.getSchemaLess()
                                    .booleanValue() == schemaLess.booleanValue())
                    {
                        params.add(new Object[] {
                                harness, testCase, harness.getName() + "#"
                                                   + testCase.getName()
                                                   + (" schema-" + (schemaLess ? "less"
                                                           : "full")
                                                      + ")"),
                                schemaLess });
                    }
                }
            }

        }

        return params;
    }

    public TestHarnessRunner(TestHarness harness, TestCase testCase, @SuppressWarnings("unused") String name, boolean schemaLess)
    {
        this.harness = harness;
        this.testCase = testCase;
        this.schemaLess = schemaLess;
    }

    @Test
    public void test()
    {
        Assume.assumeFalse("Ignored", testCase.isIgnore());
        testInternal();
    }

    // CSOFF
    private void testInternal()
    // CSON
    {
        CatalogRegistry registry = new CatalogRegistry();
        for (TestCatalog catalog : harness.getCatalogs())
        {
            registry.registerCatalog(catalog.getAlias(), new TCatalog(catalog, schemaLess));
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

        QuerySession session = new QuerySession(registry);
        session.setPrintWriter(new PrintWriter(System.out));
        session.setDefaultCatalogAlias(harness.getCatalogs()
                .get(0)
                .getAlias());

        VectorFactory vectorFactory = new VectorFactory(new BufferAllocator());
        session.setVectorFactory(vectorFactory);

        TimeZone defaultTimezone = TimeZone.getDefault();
        boolean fail = false;
        try
        {
            TimeZone.setDefault(TimeZone.getTimeZone(ZoneId.of("Europe/Berlin")));
            CompiledQuery query = Payloadbuilder.compile(session, testCase.getQuery());
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
                if (!e.getClass()
                        .isAssignableFrom(testCase.getExpectedException()))
                {
                    throw e;
                }

                if (!isBlank(testCase.getExpectedMessageContains()))
                {
                    Assert.assertTrue("Expected message to contain " + testCase.getExpectedMessageContains() + ", but got " + e.getMessage(),
                            CI.contains(e.getMessage(), testCase.getExpectedMessageContains()));
                }

                return;
            }
            else
            {
                throw e;
            }
        }
        finally
        {
            TimeZone.setDefault(defaultTimezone);

            vectorFactory.printAllocationInformation();
        }

        int size = testCase.getExpectedResultSets()
                .size();

        Assert.assertEquals((schemaLess ? "SchemaLess: "
                : "") + "Expected number of result sets to be equal", size, actualResultSets.size());

        for (int i = 0; i < size; i++)
        {
            assertResultSetEqual(testCase.isOnlyAssertExpectedColumns(), schemaLess, i, testCase.getExpectedResultSets()
                    .get(i), actualResultSets.get(i));
        }
    }

    private void assertResultSetEqual(boolean onlyAssertExpectedColumns, boolean schemaLess, int number, List<List<ColumnValue>> expected, List<List<ColumnValue>> actual)
    {
        int size = onlyAssertExpectedColumns ? expected.size()
                : Math.max(expected.size(), actual.size());
        for (int i = 0; i < size; i++)
        {
            List<ColumnValue> expectedRow = i < expected.size() ? expected.get(i)
                    : emptyList();
            List<ColumnValue> actualRow = i < actual.size() ? actual.get(i)
                    : emptyList();

            int rowSize = onlyAssertExpectedColumns ? expectedRow.size()
                    : Math.max(expectedRow.size(), actualRow.size());
            for (int j = 0; j < rowSize; j++)
            {
                ColumnValue expectedColumn = j < expectedRow.size() ? expectedRow.get(j)
                        : null;
                ColumnValue actualColumn = j < actualRow.size() ? actualRow.get(j)
                        : null;

                if (!equals(expectedColumn, actualColumn))
                {
                    fail((schemaLess ? "SchemaLess: "
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
        if (expected == null
                && actual != null)
        {
            return false;
        }
        else if (expected != null
                && actual == null)
        {
            return false;
        }
        else if (!Objects.equals(expected.getKey(), actual.getKey()))
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

        if ("###IGNORE###".equals(expected.getValue()))
        {
            return true;
        }

        // toString-ify the actual if we expect a string
        if (expected.getValue() instanceof String
                && !(actual.getValue() instanceof String))
        {
            return Objects.equals(expected.getValue(), String.valueOf(actual.getValue()));
        }

        // Use expression math to handle float = double etc.
        try
        {
            if (ExpressionMath.cmp(expected.getValue(), actual.getValue()) == 0)
            {
                return true;
            }
        }
        catch (IllegalArgumentException e)
        {
            // Swallow this and continue with Objects.equals
        }

        return Objects.equals(expected.getValue(), actual.getValue());
    }

    /** Harness catalog */
    private static class TCatalog extends Catalog
    {
        private static final Schema SYS_TABLES_SCHEMA = Schema.of(Column.of(SYS_TABLES_NAME, Type.String), Column.of("columns", ResolvedType.array(ResolvedType.of(Type.String))));
        private static final Schema SYS_COLUMNS_SCHEMA = Schema.of(Column.of(SYS_COLUMNS_TABLE, Type.String), Column.of(SYS_COLUMNS_NAME, Type.String), Column.of("custom", Type.Int));
        private static final Schema SYS_INDICES_SCHEMA = Schema.EMPTY;
        private final TestCatalog catalog;
        private final boolean schemaLess;

        TCatalog(TestCatalog catalog, boolean schemaLess)
        {
            super("Test#" + catalog.getAlias());
            this.catalog = catalog;
            this.schemaLess = schemaLess;

            registerFunction(new ScalarFunctionInfo("testFunc", FunctionType.SCALAR)
            {
            });

            registerFunction(new TableFunctionInfo("testTVFOptions")
            {
                @Override
                public Arity arity()
                {
                    return Arity.ZERO;
                }

                @Override
                public TupleIterator execute(IExecutionContext context, String catalogAlias, Optional<Schema> schema, List<IExpression> arguments, List<Option> options)
                {
                    // Returns a tuple vector with all options evaluated
                    List<Column> columns = new ArrayList<>();
                    List<ValueVector> vectors = new ArrayList<>();
                    for (Option option : options)
                    {
                        columns.add(new Column(option.getOption()
                                .toDotDelimited(),
                                option.getValueExpression()
                                        .getType()));
                        vectors.add(option.getValueExpression()
                                .eval(context));
                    }
                    final Schema s = new Schema(columns);
                    return TupleIterator.singleton(new TupleVector()
                    {
                        @Override
                        public Schema getSchema()
                        {
                            return s;
                        }

                        @Override
                        public int getRowCount()
                        {
                            return columns.size();
                        }

                        @Override
                        public ValueVector getColumn(int column)
                        {
                            return vectors.get(column);
                        }
                    });
                }
            });
        }

        private TestTable getTestTable(QualifiedName table)
        {
            String tableName = table.getLast();
            for (TestTable testTable : catalog.getTables())
            {
                if (CI.contains(testTable.getName(), tableName))
                {
                    return testTable;
                }
            }

            throw new IllegalArgumentException("No test table setup with name: " + table.toString());
        }

        private Schema getSchema(TestTable table)
        {
            return new Schema(table.getColumns()
                    .stream()
                    .map(c -> Column.of(c, ResolvedType.of(Type.Any)))
                    .collect(toList()));
        }

        @Override
        public TableSchema getTableSchema(IQuerySession session, String catalogAlias, QualifiedName table, List<Option> options)
        {
            if (schemaLess)
            {
                return TableSchema.EMPTY;
            }

            TestTable testTable = getTestTable(table);
            return new TableSchema(getSchema(testTable));
        }

        @Override
        public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
        {
            final TestTable testTable = getTestTable(table);
            final Schema schema = getSchema(testTable);
            final List<Object[]> rows = testTable.getRows();
            final int rowCount = rows.size();
            final Projection projection = data.getProjection();

            return new IDatasource()
            {
                @Override
                public TupleIterator execute(IExecutionContext context)
                {
                    QualifiedName optionName = QualifiedName.of("test_array");
                    ValueVector option = context.getOption(optionName, data.getOptions());
                    if (option != null)
                    {
                        ((ExecutionContext) context).setVariable(optionName.getFirst(), option);
                    }

                    ObjectTupleVector tupleVector = new ObjectTupleVector(data.getSchema()
                            .orElse(schema), rowCount, (row, col) ->
                            {
                                Column column = schema.getColumns()
                                        .get(col);

                                // Return values based on projection for this datasource
                                if (projection.type() == ProjectionType.NONE)
                                {
                                    return null;
                                }
                                else if (projection.type() == ProjectionType.COLUMNS
                                        && !projection.columns()
                                                .contains(column.getName()))
                                {
                                    return null;
                                }

                                return rows.get(row)[col];
                            });
                    return TupleIterator.singleton(tupleVector);
                }
            };
        }

        @Override
        public TableSchema getSystemTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
        {
            String type = table.getLast();
            if (SYS_TABLES.equalsIgnoreCase(type))
            {
                return new TableSchema(SYS_TABLES_SCHEMA);
            }
            else if (SYS_COLUMNS.equalsIgnoreCase(type))
            {
                return new TableSchema(SYS_COLUMNS_SCHEMA);
            }
            else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
            {
                return new TableSchema(SYS_FUNCTIONS_SCHEMA);
            }
            else if (SYS_INDICES.equalsIgnoreCase(type))
            {
                return new TableSchema(SYS_INDICES_SCHEMA);
            }

            return super.getSystemTableSchema(session, catalogAlias, table);
        }

        @Override
        public IDatasource getSystemTableDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
        {
            TupleVector vector = null;
            String type = table.getLast();
            if (SYS_TABLES.equalsIgnoreCase(type))
            {
                List<TestTable> tables = catalog.getTables();
                vector = new ObjectTupleVector(data.getSchema()
                        .get(), tables.size(), (row, col) ->
                        {
                            TestTable t = tables.get(row);
                            if (col == 0)
                            {
                                return t.getName();
                            }
                            return new ValueVector()
                            {
                                @Override
                                public ResolvedType type()
                                {
                                    return ResolvedType.of(Type.String);
                                }

                                @Override
                                public int size()
                                {
                                    return t.getColumns()
                                            .size();
                                }

                                @Override
                                public boolean isNull(int row)
                                {
                                    return t.getColumns()
                                            .get(row) == null;
                                }

                                @Override
                                public Object getAny(int row)
                                {
                                    return t.getColumns()
                                            .get(row);
                                }
                            };
                        });
            }
            else if (SYS_COLUMNS.equalsIgnoreCase(type))
            {
                List<Pair<TestTable, String>> columns = catalog.getTables()
                        .stream()
                        .flatMap(t -> t.getColumns()
                                .stream()
                                .map(c -> Pair.of(t, c)))
                        .collect(toList());
                vector = new ObjectTupleVector(data.getSchema()
                        .get(), columns.size(), (row, col) ->
                        {
                            Pair<TestTable, String> p = columns.get(row);
                            if (col == 0)
                            {
                                return p.getKey()
                                        .getName();
                            }
                            else if (col == 1)
                            {
                                return p.getValue();
                            }
                            return p.getValue()
                                    .length();
                        });
            }
            else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
            {
                vector = getFunctionsTupleVector(data.getSchema()
                        .get());
            }
            else if (SYS_INDICES.equalsIgnoreCase(type))
            {
                vector = TupleVector.EMPTY;
            }

            if (vector == null)
            {
                return super.getSystemTableDataSource(session, catalogAlias, table, data);
            }

            final TupleVector tupleVector = vector;
            return new IDatasource()
            {
                @Override
                public TupleIterator execute(IExecutionContext context)
                {
                    return TupleIterator.singleton(tupleVector);
                }
            };
        }
    }
}
