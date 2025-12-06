package se.kuseman.payloadbuilder.core.test;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.Strings.CI;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeFalse;

import java.io.IOException;
import java.io.InputStream;
import java.io.PrintWriter;
import java.time.ZoneId;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.function.Supplier;
import java.util.stream.IntStream;

import org.apache.commons.lang3.tuple.Pair;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

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
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.InsertIntoData;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.SelectIntoData;
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
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.QueryResult;
import se.kuseman.payloadbuilder.core.RawQueryResult;
import se.kuseman.payloadbuilder.core.RawQueryResult.ResultConsumer;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.ExpressionMath;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;
import se.kuseman.payloadbuilder.core.execution.vector.BufferAllocator;
import se.kuseman.payloadbuilder.core.execution.vector.VectorFactory;
import se.kuseman.payloadbuilder.core.physicalplan.PlanUtils;
import se.kuseman.payloadbuilder.core.test.AObjectOutputWriter.ColumnValue;

/** Harness runner test */
class TestHarnessRunner
{
    static final ObjectMapper MAPPER = new ObjectMapper().configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);

    private static List<Object[]> baseConstructs()
    {
        return testHarness("BaseConstructs.json");
    }

    private static List<Object[]> joins()
    {
        return testHarness("Joins.json");
    }

    private static List<Object[]> systemFunctions()
    {
        return testHarness("SystemFunctions.json");
    }

    private static List<Object[]> temporaryTables()
    {
        return testHarness("TemporaryTables.json");
    }

    /** Create arguments for provided harness filename. */
    private static List<Object[]> testHarness(String filename)
    {
        TestHarness harness;

        String resource = "/harnessCases/" + filename;
        InputStream stream = TestHarnessRunner.class.getResourceAsStream(resource);
        if (stream == null)
        {
            fail("No harness resouce found: " + resource);
        }
        try
        {
            harness = MAPPER.readValue(stream, TestHarness.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error reading harness file " + filename, e);
        }

        List<Object[]> params = new ArrayList<>();
        for (TestCase testCase : harness.getCases())
        {
            for (Boolean schemaLess : asList(true, false))
            {
                if (testCase.getSchemaLess() == null
                        || testCase.getSchemaLess()
                                .booleanValue() == schemaLess.booleanValue())
                {
                    for (Boolean typedVectors : asList(true, false))
                    {
                        if (testCase.getTypedVectors() == null
                                || testCase.getTypedVectors()
                                        .booleanValue() == typedVectors.booleanValue())
                        {
                            String name = "%s#%s#%s".formatted(testCase.getName(), schemaLess ? "schema-less"
                                    : "schema-full",
                                    typedVectors ? "typed-vectors"
                                            : "any-vectors");

                            params.add(new Object[] { harness, testCase, name, schemaLess, typedVectors });
                        }
                    }
                }
            }
        }
        return params;
    }

    // CSOFF
    @ParameterizedTest(
            name = "{2}")
    @MethodSource("baseConstructs")
    void baseConstructs(TestHarness harness, TestCase testCase, String name, boolean schemaLess, boolean typedVectors)
    {
        assumeFalse(testCase.isIgnore(), "Ignored");
        testInternal(harness, testCase, name, schemaLess, typedVectors);
    }

    @ParameterizedTest(
            name = "{2}")
    @MethodSource("joins")
    void joins(TestHarness harness, TestCase testCase, String name, boolean schemaLess, boolean typedVectors)
    {
        assumeFalse(testCase.isIgnore(), "Ignored");
        testInternal(harness, testCase, name, schemaLess, typedVectors);
    }

    @ParameterizedTest(
            name = "{2}")
    @MethodSource("systemFunctions")
    void systemFunction(TestHarness harness, TestCase testCase, String name, boolean schemaLess, boolean typedVectors)
    {
        assumeFalse(testCase.isIgnore(), "Ignored");
        testInternal(harness, testCase, name, schemaLess, typedVectors);
    }

    @ParameterizedTest(
            name = "{2}")
    @MethodSource("temporaryTables")
    void temporaryTables(TestHarness harness, TestCase testCase, String name, boolean schemaLess, boolean typedVectors)
    {
        assumeFalse(testCase.isIgnore(), "Ignored");
        testInternal(harness, testCase, name, schemaLess, typedVectors);
    }
    // CSON

    // CSOFF
    private void testInternal(TestHarness harness, TestCase testCase, String name, boolean schemaLess, boolean typedVectors)
    // CSON
    {
        CatalogRegistry registry = new CatalogRegistry();
        for (TestCatalog catalog : harness.getCatalogs())
        {
            registry.registerCatalog(catalog.getAlias(), new TCatalog(catalog, schemaLess, typedVectors));
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

            assertSchemas(session, query, testCase, schemaLess, typedVectors);
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
                    assertTrue(CI.contains(e.getMessage(), testCase.getExpectedMessageContains()),
                            "Expected message to contain " + testCase.getExpectedMessageContains() + ", but got " + e.getMessage());
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

        assertEquals(size, actualResultSets.size(), (schemaLess ? "SchemaLess: "
                : "") + "Expected number of result sets to be equal");

        for (int i = 0; i < size; i++)
        {
            assertResultSetEqual(testCase.isOnlyAssertExpectedColumns(), schemaLess, i, testCase.getExpectedResultSets()
                    .get(i), actualResultSets.get(i));
        }
    }

    private void assertSchemas(QuerySession session, CompiledQuery query, TestCase testCase, boolean schemaLess, boolean typedVectors)
    {
        List<Schema> actual = new ArrayList<>();
        List<Schema> expected = new ArrayList<>();
        boolean runtime = false;

        if (!typedVectors
                && !testCase.getExpectedRuntimeSchemasAnyVectors()
                        .isEmpty())
        {
            expected.addAll(testCase.getExpectedRuntimeSchemasAnyVectors());
            runtime = true;
        }
        else if (typedVectors
                && !testCase.getExpectedRuntimeSchemasTypedVectors()
                        .isEmpty())
        {
            expected.addAll(testCase.getExpectedRuntimeSchemasTypedVectors());
            runtime = true;
        }

        if (!expected.isEmpty())
        {
            RawQueryResult result = query.executeRaw(session);
            while (result.hasMoreResults())
            {
                result.consumeResult(new ResultConsumer()
                {
                    @Override
                    public void schema(Schema schema)
                    {
                    }

                    @Override
                    public boolean consume(TupleVector vector)
                    {
                        // We assume only one vector per query
                        actual.add(vector.getSchema());
                        return false;
                    }
                });
            }

            String prefix = (schemaLess ? "SchemaLess"
                    : "");
            prefix += (runtime ? " Runtime "
                    : " : ");
            assertEquals(expected.size(), actual.size(), prefix + "Expected number of result schemas to be equal");

            for (int i = 0; i < expected.size(); i++)
            {
                assertSchemaEquals(prefix + "Schema " + i + ": ", expected.get(i), actual.get(i));
            }
        }
    }

    private void assertSchemaEquals(String prefix, Schema expected, Schema actual)
    {
        int size = expected.getSize();
        assertEquals(size, actual.getSize(), prefix + "Expected number of columns to be equal");

        for (int j = 0; j < size; j++)
        {
            Column expectedColumn = expected.getColumns()
                    .get(j);
            Column actualColumn = actual.getColumns()
                    .get(j);

            assertEquals(expectedColumn.getName(), actualColumn.getName(), prefix + "Expected name of column: " + j + " to be equal");
            assertEquals(expectedColumn.getType()
                    .getType(),
                    actualColumn.getType()
                            .getType(),
                    prefix + "Expected type of column: " + expectedColumn.getName() + " to be equal");
            assertEquals(expectedColumn.getMetaData(), actualColumn.getMetaData(), prefix + "Expected metaData of column: " + expectedColumn.getName() + " to be equal");

            // Recursive compare of complex type
            if (expectedColumn.getType()
                    .getSchema() != null)
            {
                assertSchemaEquals(prefix + "Column: " + expectedColumn.getName() + " ", expectedColumn.getType()
                        .getSchema(),
                        actualColumn.getType()
                                .getSchema());
            }
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
        private final boolean typedVectors;

        /** Tables created via Select Into */
        private Map<String, TestTable> createdTables = new HashMap<>();

        TCatalog(TestCatalog catalog, boolean schemaLess, boolean typedVectors)
        {
            super("Test#" + catalog.getAlias());
            this.catalog = catalog;
            this.schemaLess = schemaLess;
            this.typedVectors = typedVectors;

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
                public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData data)
                {
                    // Returns a tuple vector with all options evaluated
                    List<Column> columns = new ArrayList<>();
                    List<ValueVector> vectors = new ArrayList<>();
                    for (Option option : data.getOptions())
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

        private TestTable getTestTable(QualifiedName table, boolean searchInCreatedTables)
        {
            String tableName = table.getLast();
            for (TestTable testTable : catalog.getTables())
            {
                if (CI.contains(testTable.getName(), tableName))
                {
                    return testTable;
                }
            }

            if (searchInCreatedTables)
            {
                TestTable testTable = createdTables.get(tableName);
                if (testTable != null)
                {
                    return testTable;
                }
            }

            if (searchInCreatedTables)
            {
                throw new IllegalArgumentException("No test table setup with name: " + table.toString());
            }
            return null;
        }

        private Schema getSchema(TestTable table)
        {
            if (typedVectors)
            {
                if (table.getTypes()
                        .isEmpty())
                {
                    throw new IllegalArgumentException("Missing types for table: " + table.getName());
                }
            }

            int size = table.getColumns()
                    .size();
            return new Schema(IntStream.range(0, size)
                    .mapToObj(i -> new Column(table.getColumns()
                            .get(i),
                            typedVectors ? table.getTypes()
                                    .get(i)
                                    : ResolvedType.of(Type.Any),
                            new Column.MetaData(Map.of("scale", i, "name", table.getColumns()
                                    .get(i), "table", table.getName()))))
                    .collect(toList()));
        }

        @Override
        public TableSchema getTableSchema(IExecutionContext context, String catalogAlias, QualifiedName table, List<Option> options)
        {
            // Verify that catalogAlias is provided and is not empty in case of session default catalog
            assertTrue(isNotBlank(catalogAlias), "catalogAlias should not be empty");

            if (schemaLess)
            {
                return TableSchema.EMPTY;
            }

            TestTable testTable = getTestTable(table, true);
            return new TableSchema(getSchema(testTable));
        }

        @Override
        public IDatasink getInsertIntoSink(IQuerySession session, String catalogAlias, QualifiedName table, InsertIntoData data)
        {
            // Verify that catalogAlias is provided and is not empty in case of session default catalog
            assertTrue(isNotBlank(catalogAlias), "catalogAlias should not be empty");

            TestTable testTable = createdTables.get(table.getLast());
            if (testTable == null)
            {
                throw new QueryException("Table " + table + " does not exist");
            }

            return new IDatasink()
            {
                @Override
                public void execute(IExecutionContext context, TupleIterator input)
                {
                    TupleVector vector = PlanUtils.concat(context, input);
                    int rowCount = vector.getRowCount();
                    int colCount = vector.getSchema()
                            .getSize();
                    for (int i = 0; i < rowCount; i++)
                    {
                        Object[] row = new Object[colCount];
                        testTable.getRows()
                                .add(row);
                        for (int j = 0; j < colCount; j++)
                        {
                            row[j] = vector.getColumn(j)
                                    .valueAsObject(i);
                        }
                    }
                }
            };
        }

        @Override
        public IDatasink getSelectIntoSink(IQuerySession session, String catalogAlias, QualifiedName table, SelectIntoData data)
        {
            // Verify that catalogAlias is provided and is not empty in case of session default catalog
            assertTrue(isNotBlank(catalogAlias), "catalogAlias should not be empty");

            TestTable existingTestTable = getTestTable(table, false);
            if (existingTestTable != null)
            {
                throw new QueryException("Table " + table + " already exists");
            }

            // Insert a shell of the new table that will be accessible during planning before execution
            TestTable testTable = new TestTable();
            testTable.setName(table.getLast());
            testTable.setColumns(data.getInputSchema()
                    .getColumns()
                    .stream()
                    .map(c -> c.getName())
                    .toList());
            testTable.setTypes(data.getInputSchema()
                    .getColumns()
                    .stream()
                    .map(c -> c.getType())
                    .toList());
            testTable.setRows(new ArrayList<>());
            createdTables.put(table.getLast(), testTable);

            return new IDatasink()
            {
                @Override
                public void execute(IExecutionContext context, TupleIterator input)
                {
                    TupleVector vector = PlanUtils.concat(context, input);
                    Schema schema = vector.getSchema();

                    // Set the runtime data of the test table
                    testTable.setColumns(schema.getColumns()
                            .stream()
                            .map(c -> c.getName())
                            .toList());
                    testTable.setTypes(schema.getColumns()
                            .stream()
                            .map(c -> c.getType())
                            .toList());

                    int rowCount = vector.getRowCount();
                    int colCount = schema.getSize();
                    for (int i = 0; i < rowCount; i++)
                    {
                        Object[] row = new Object[colCount];
                        testTable.getRows()
                                .add(row);
                        for (int j = 0; j < colCount; j++)
                        {
                            row[j] = vector.getColumn(j)
                                    .valueAsObject(i);
                        }
                    }
                }
            };
        }

        @Override
        public void dropTable(IQuerySession session, String catalogAlias, QualifiedName table, boolean lenient)
        {
            // Verify that catalogAlias is provided and is not empty in case of session default catalog
            assertTrue(isNotBlank(catalogAlias), "catalogAlias should not be empty");

            if (createdTables.remove(table.getLast()) == null
                    && !lenient)
            {
                throw new QueryException("Table " + table + " does not exists or cannot be removed.");
            }
        }

        @Override
        public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
        {
            // Verify that catalogAlias is provided and is not empty in case of session default catalog
            assertTrue(isNotBlank(catalogAlias), "catalogAlias should not be empty");

            final TestTable testTable = getTestTable(table, true);
            final List<Object[]> rows = testTable.getRows();
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

                    Schema schema = getSchema(testTable);
                    ObjectTupleVector tupleVector = new ObjectTupleVector(schema, rows.size(), (row, col) ->
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

                        Object value = rows.get(row)[col];

                        if (typedVectors
                                && column.getType()
                                        .getType() == Column.Type.Table)
                        {
                            @SuppressWarnings("unchecked")
                            final List<Map<String, Object>> table = (List<Map<String, Object>>) value;
                            final int rowCount = table.size();
                            final Schema tableSchema = column.getType()
                                    .getSchema();
                            return new ObjectTupleVector(tableSchema, rowCount, (r, c) ->
                            {
                                Map<String, Object> m = table.get(r);
                                String mc = tableSchema.getColumns()
                                        .get(c)
                                        .getName();
                                return m.get(mc);
                            });
                        }
                        else if (typedVectors
                                && column.getType()
                                        .getType() == Column.Type.Object)
                        {
                            return VectorUtils.convertToObjectVector(value);
                        }

                        return value;
                    });
                    return TupleIterator.singleton(tupleVector);
                }
            };
        }

        @Override
        public TableSchema getSystemTableSchema(IQuerySession session, String catalogAlias, QualifiedName table)
        {
            // Verify that catalogAlias is provided and is not empty in case of session default catalog
            assertTrue(isNotBlank(catalogAlias), "catalogAlias should not be empty");

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
            // Verify that catalogAlias is provided and is not empty in case of session default catalog
            assertTrue(isNotBlank(catalogAlias), "catalogAlias should not be empty");

            Supplier<TupleVector> vector = null;
            String type = table.getLast();
            if (SYS_TABLES.equalsIgnoreCase(type))
            {
                vector = () ->
                {
                    List<TestTable> tables = new ArrayList<>(catalog.getTables());
                    tables.addAll(createdTables.values());
                    return new ObjectTupleVector(SYS_TABLES_SCHEMA, tables.size(), (row, col) ->
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
                };
            }
            else if (SYS_COLUMNS.equalsIgnoreCase(type))
            {
                vector = () ->
                {
                    List<Pair<TestTable, String>> columns = catalog.getTables()
                            .stream()
                            .flatMap(t -> t.getColumns()
                                    .stream()
                                    .map(c -> Pair.of(t, c)))
                            .collect(toList());

                    return new ObjectTupleVector(SYS_COLUMNS_SCHEMA, columns.size(), (row, col) ->
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
                };
            }
            else if (SYS_FUNCTIONS.equalsIgnoreCase(type))
            {
                vector = () -> getFunctionsTupleVector(SYS_FUNCTIONS_SCHEMA);
            }
            else if (SYS_INDICES.equalsIgnoreCase(type))
            {
                vector = () -> TupleVector.EMPTY;
            }

            if (vector == null)
            {
                return super.getSystemTableDataSource(session, catalogAlias, table, data);
            }

            final Supplier<TupleVector> sup = vector;
            return new IDatasource()
            {
                @Override
                public TupleIterator execute(IExecutionContext context)
                {
                    return TupleIterator.singleton(sup.get());
                }
            };
        }
    }
}
