package se.kuseman.payloadbuilder.catalog.fs;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyList;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.io.FileUtils;
import org.junit.jupiter.api.Test;
import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.CompileException;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.InsertIntoData;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.TestUtils;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.ExpressionFactory;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.expression.LiteralBooleanExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link FilesystemCatalog}. */
class FilesystemCatalogTest
{
    private static final String CATALOG_ALIAS = "fs";
    private FilesystemCatalog c = new FilesystemCatalog();
    private Catalog systemCatalogMock = Mockito.mock(Catalog.class);

    @Test
    void test_table_schema_invalid_table_name()
    {
        IExecutionContext context = mockExecutionContext();
        assertThrows(CompileException.class, () -> c.getTableSchema(context, CATALOG_ALIAS, QualifiedName.of("path", "list.csv"), emptyList()));
    }

    @Test
    void test_scanDatasource_invalid_table_name()
    {
        IExecutionContext context = mockExecutionContext();
        assertThrows(IllegalArgumentException.class,
                () -> c.getScanDataSource(context.getSession(), CATALOG_ALIAS, QualifiedName.of("path", "list.csv"), new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, emptyList())));
    }

    @Test
    void test_dropTable_invalid_table_name()
    {
        IExecutionContext context = mockExecutionContext();
        assertThrows(IllegalArgumentException.class, () -> c.dropTable(context.getSession(), CATALOG_ALIAS, QualifiedName.of("path", "list.csv"), true));
    }

    @Test
    void test_table_schema()
    {
        IExecutionContext context = mockExecutionContext();
        TableFunctionInfo functionInfo = Mockito.mock(TableFunctionInfo.class);
        when(functionInfo.getSchema(eq(context), eq(CATALOG_ALIAS), anyList(), anyList())).thenReturn(Schema.EMPTY);
        when(systemCatalogMock.getTableFunction("OPENCSV")).thenReturn(functionInfo);
        TableSchema tableSchema = c.getTableSchema(context, CATALOG_ALIAS, QualifiedName.of("list.csv"), emptyList());
        assertEquals(TableSchema.EMPTY, tableSchema);
    }

    @Test
    void test_table_schema_options_are_forwarded()
    {
        List<Option> options = List.of(new Option(QualifiedName.of("columnHeaders"), new LiteralStringExpression("col1,col2")));

        IExecutionContext context = mockExecutionContext();
        TableFunctionInfo functionInfo = Mockito.mock(TableFunctionInfo.class);
        when(functionInfo.getSchema(eq(context), eq(CATALOG_ALIAS), anyList(), eq(options))).thenReturn(Schema.of(Column.of("col1", Type.String), Column.of("col2", Type.String)));
        when(systemCatalogMock.getTableFunction("OPENJSON")).thenReturn(functionInfo);

        TableSchema tableSchema = c.getTableSchema(context, CATALOG_ALIAS, QualifiedName.of("list.json"), options);
        assertEquals(new TableSchema(Schema.of(Column.of("col1", Type.String), Column.of("col2", Type.String))), tableSchema);
    }

    @Test
    void test_table_schema_default_are_forwarded_as_csv()
    {
        IExecutionContext context = mockExecutionContext();
        TableFunctionInfo functionInfo = Mockito.mock(TableFunctionInfo.class);
        when(functionInfo.getSchema(eq(context), eq(CATALOG_ALIAS), anyList(), anyList())).thenReturn(Schema.of(Column.of("column", Type.String)));
        when(systemCatalogMock.getTableFunction("OPENCSV")).thenReturn(functionInfo);

        TableSchema tableSchema = c.getTableSchema(context, CATALOG_ALIAS, QualifiedName.of("list.txt"), emptyList());
        assertEquals(new TableSchema(Schema.of(Column.of("column", Type.String))), tableSchema);
    }

    @Test
    void test_table_schema_filetype_option()
    {
        IExecutionContext context = mockExecutionContext();
        TableFunctionInfo functionInfo = Mockito.mock(TableFunctionInfo.class);
        when(functionInfo.getSchema(eq(context), eq(CATALOG_ALIAS), anyList(), anyList())).thenReturn(Schema.of(Column.of("column2", Type.String)));
        when(systemCatalogMock.getTableFunction("OPENCSV")).thenReturn(functionInfo);

        //@formatter:off
        TableSchema tableSchema = c.getTableSchema(context, CATALOG_ALIAS, QualifiedName.of("list.unknown"),
                List.of(new Option(FilesystemCatalog.FORMAT, new LiteralStringExpression("cSv"))));
        //@formatter:on
        assertEquals(new TableSchema(Schema.of(Column.of("column2", Type.String))), tableSchema);
    }

    @Test
    void test_table_schema_fallback_type()
    {
        IExecutionContext context = TestUtils.mockExecutionContext(CATALOG_ALIAS, emptyMap(), 0, new NodeData());
        //@formatter:off
        TableSchema tableSchema = c.getTableSchema(context, CATALOG_ALIAS, QualifiedName.of("list.unknown"), emptyList());
        //@formatter:on
        assertEquals(new TableSchema(Schema.of(Column.of("value", Type.String))), tableSchema);
    }

    @Test
    void test_table_schema_filename_in_catalog_property()
    {
        String filekey = "filekey";
        IExecutionContext context = mockExecutionContext(Map.of(filekey, "list.xml"));
        TableFunctionInfo functionInfo = Mockito.mock(TableFunctionInfo.class);
        when(functionInfo.getSchema(eq(context), eq(CATALOG_ALIAS), anyList(), anyList())).thenReturn(Schema.of(Column.of("column3", Type.Int)));
        when(systemCatalogMock.getTableFunction("OPENXML")).thenReturn(functionInfo);
        //@formatter:off
        TableSchema tableSchema = c.getTableSchema(context, CATALOG_ALIAS, QualifiedName.of(filekey), emptyList());
        //@formatter:on
        assertEquals(new TableSchema(Schema.of(Column.of("column3", Type.Int))), tableSchema);
    }

    @Test
    void test_datasource() throws IOException
    {
        // Use the real systemcatalog here
        IExecutionContext context = TestUtils.mockExecutionContext(CATALOG_ALIAS, emptyMap(), 0, null);

        Path tempFile = Files.createTempFile("fscatalog", "csv");
        FileUtils.write(tempFile.toFile(), """
                col1,col2
                10,20
                30,40
                hello,world
                """, StandardCharsets.UTF_8);

        QualifiedName tableName = QualifiedName.of(tempFile.toAbsolutePath()
                .toString());

        IDatasource dataSource = c.getScanDataSource(context.getSession(), CATALOG_ALIAS, tableName,
                new DatasourceData(0, emptyList(), emptyList(), Projection.ALL, List.of(new Option(FilesystemCatalog.FORMAT, new LiteralStringExpression("cSv")))));

        TupleIterator it = dataSource.execute(context);
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();
            rowCount += next.getRowCount();
            assertEquals(Schema.of(Column.of("col1", Type.String), Column.of("col2", Type.String)), next.getSchema());
            VectorTestUtils.assertVectorsEquals(VectorTestUtils.vv(Type.String, "10", "30", "hello"), next.getColumn(0));
            VectorTestUtils.assertVectorsEquals(VectorTestUtils.vv(Type.String, "20", "40", "world"), next.getColumn(1));
        }
        it.close();
        assertEquals(3, rowCount);

        Path parent = tempFile.getParent();

        c.dropTable(context.getSession(), CATALOG_ALIAS, tableName, false);

        assertFalse(Files.exists(tempFile));

        c.dropTable(context.getSession(), CATALOG_ALIAS, tableName, true);

        assertThrows(IllegalArgumentException.class, () -> c.dropTable(context.getSession(), CATALOG_ALIAS, tableName, false));
        // Directories should not be removed
        assertThrows(IllegalArgumentException.class, () -> c.dropTable(context.getSession(), CATALOG_ALIAS, QualifiedName.of(parent.toAbsolutePath()
                .toString()), false));
    }

    @Test
    void test_datasink_selectinto_csv() throws IOException
    {
        CatalogRegistry catalogRegistry = new CatalogRegistry();
        catalogRegistry.registerCatalog("fs", c);
        ExecutionContext context = new ExecutionContext(new QuerySession(catalogRegistry));

        Path tempFile = Files.createTempFile("fsCatalogSink", "csv");
        tempFile = Files.move(tempFile, Path.of(tempFile.toString() + ".csv"));

        Files.deleteIfExists(tempFile);
        QualifiedName tableName = QualifiedName.of(tempFile.toAbsolutePath()
                .toString());
        IDatasink sink = c.getSelectIntoSink(context.getSession(), CATALOG_ALIAS, tableName, new InsertIntoData(0, emptyList(), emptyList(), emptyList()));
        // @format:off
        TupleVector vector1 = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
                List.of(VectorTestUtils.vv(Type.Int, 10, 20, 30), VectorTestUtils.vv(Type.String, "ten", "twenty", "thirty åäö")));
        TupleVector vector2 = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
                List.of(VectorTestUtils.vv(Type.Int, 100, 200, 300), VectorTestUtils.vv(Type.String, "hundret", "two hundret", "three hundred")));
        // @format:on

        sink.execute(context, new TupleIterator()
        {
            int index = 0;

            @Override
            public TupleVector next()
            {
                if (index > 2)
                {
                    throw new NoSuchElementException();
                }
                return index == 1 ? vector1
                        : vector2;
            }

            @Override
            public boolean hasNext()
            {
                return index++ <= 1;
            }
        });

        assertEquals("""
                intCol,stringCol
                10,ten
                20,twenty
                30,thirty åäö
                100,hundret
                200,two hundret
                300,three hundred
                """, FileUtils.readFileToString(tempFile.toFile(), StandardCharsets.UTF_8));
        Files.delete(tempFile);
    }

    @Test
    void test_datasink_selectinto_txt() throws IOException
    {
        CatalogRegistry catalogRegistry = new CatalogRegistry();
        catalogRegistry.registerCatalog("fs", c);
        ExecutionContext context = new ExecutionContext(new QuerySession(catalogRegistry));

        Path tempFile = Files.createTempFile("fsCatalogSink", "txt");
        tempFile = Files.move(tempFile, Path.of(tempFile.toString() + ".txt"));

        Files.deleteIfExists(tempFile);
        QualifiedName tableName = QualifiedName.of(tempFile.toAbsolutePath()
                .toString());
        IDatasink sink = c.getSelectIntoSink(context.getSession(), CATALOG_ALIAS, tableName, new InsertIntoData(0, emptyList(), emptyList(), emptyList()));
        // @format:off
        TupleVector vector1 = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
                List.of(VectorTestUtils.vv(Type.Int, 10, 20, 30), VectorTestUtils.vv(Type.String, "ten", "twenty", "thirty åäö")));
        TupleVector vector2 = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
                List.of(VectorTestUtils.vv(Type.Int, 100, 200, 300), VectorTestUtils.vv(Type.String, "hundret", "two hundret", "three hundred")));
        // @format:on

        sink.execute(context, new TupleIterator()
        {
            int index = 0;

            @Override
            public TupleVector next()
            {
                if (index > 2)
                {
                    throw new NoSuchElementException();
                }
                return index == 1 ? vector1
                        : vector2;
            }

            @Override
            public boolean hasNext()
            {
                return index++ <= 1;
            }
        });

        assertEquals("""
                intCol stringCol
                10 ten
                20 twenty
                30 thirty åäö
                100 hundret
                200 two hundret
                300 three hundred
                """, FileUtils.readFileToString(tempFile.toFile(), StandardCharsets.UTF_8)
                .replaceAll("\\r", ""));
        Files.delete(tempFile);
    }

    @Test
    void test_datasink_selectinto_fallback() throws IOException
    {
        CatalogRegistry catalogRegistry = new CatalogRegistry();
        catalogRegistry.registerCatalog("fs", c);
        ExecutionContext context = new ExecutionContext(new QuerySession(catalogRegistry));

        Path tempFile = Files.createTempFile("fsCatalogSink", "dum");
        tempFile = Files.move(tempFile, Path.of(tempFile.toString() + ".dum"));

        Files.deleteIfExists(tempFile);
        QualifiedName tableName = QualifiedName.of(tempFile.toAbsolutePath()
                .toString());
        IDatasink sink = c.getSelectIntoSink(context.getSession(), CATALOG_ALIAS, tableName, new InsertIntoData(0, emptyList(), emptyList(), emptyList()));
        // @format:off
        TupleVector vector1 = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
                List.of(VectorTestUtils.vv(Type.Int, 10, 20, 30), VectorTestUtils.vv(Type.String, "ten", "twenty", "thirty åäö")));
        TupleVector vector2 = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
                List.of(VectorTestUtils.vv(Type.Int, 100, 200, 300), VectorTestUtils.vv(Type.String, "hundret", "two hundret", "three hundred")));
        // @format:on

        sink.execute(context, new TupleIterator()
        {
            int index = 0;

            @Override
            public TupleVector next()
            {
                if (index > 2)
                {
                    throw new NoSuchElementException();
                }
                return index == 1 ? vector1
                        : vector2;
            }

            @Override
            public boolean hasNext()
            {
                return index++ <= 1;
            }
        });

        assertEquals("""
                intCol stringCol
                10 ten
                20 twenty
                30 thirty åäö
                100 hundret
                200 two hundret
                300 three hundred
                """, FileUtils.readFileToString(tempFile.toFile(), StandardCharsets.UTF_8)
                .replaceAll("\\r", ""));
        Files.delete(tempFile);
    }

    @Test
    void test_datasink_selectinto_format_option() throws IOException
    {
        CatalogRegistry catalogRegistry = new CatalogRegistry();
        catalogRegistry.registerCatalog("fs", c);
        ExecutionContext context = new ExecutionContext(new QuerySession(catalogRegistry));

        Path tempFile = Files.createTempFile("fsCatalogSink", "dum");
        tempFile = Files.move(tempFile, Path.of(tempFile.toString() + ".dum"));

        Files.deleteIfExists(tempFile);
        QualifiedName tableName = QualifiedName.of(tempFile.toAbsolutePath()
                .toString());
        IDatasink sink = c.getSelectIntoSink(context.getSession(), CATALOG_ALIAS, tableName,
                new InsertIntoData(0, emptyList(), List.of(new Option(FilesystemCatalog.FORMAT, new LiteralStringExpression("text"))), emptyList()));
        // @format:off
        TupleVector vector1 = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
                List.of(VectorTestUtils.vv(Type.Int, 10, 20, 30), VectorTestUtils.vv(Type.String, "ten", "twenty", "thirty åäö")));
        TupleVector vector2 = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
                List.of(VectorTestUtils.vv(Type.Int, 100, 200, 300), VectorTestUtils.vv(Type.String, "hundret", "two hundret", "three hundred")));
        // @format:on

        sink.execute(context, new TupleIterator()
        {
            int index = 0;

            @Override
            public TupleVector next()
            {
                if (index > 2)
                {
                    throw new NoSuchElementException();
                }
                return index == 1 ? vector1
                        : vector2;
            }

            @Override
            public boolean hasNext()
            {
                return index++ <= 1;
            }
        });

        assertEquals("""
                intCol stringCol
                10 ten
                20 twenty
                30 thirty åäö
                100 hundret
                200 two hundret
                300 three hundred
                """, FileUtils.readFileToString(tempFile.toFile(), StandardCharsets.UTF_8)
                .replaceAll("\\r", ""));
        Files.delete(tempFile);
    }

    @Test
    void test_datasink_selectinto_json() throws IOException
    {
        CatalogRegistry catalogRegistry = new CatalogRegistry();
        catalogRegistry.registerCatalog("fs", c);
        ExecutionContext context = new ExecutionContext(new QuerySession(catalogRegistry));

        Path tempFile = Files.createTempFile("fsCatalogSink", "json");
        tempFile = Files.move(tempFile, Path.of(tempFile.toString() + ".json"));
        Files.deleteIfExists(tempFile);
        QualifiedName tableName = QualifiedName.of(tempFile.toAbsolutePath()
                .toString());
        IDatasink sink = c.getSelectIntoSink(context.getSession(), CATALOG_ALIAS, tableName, new InsertIntoData(0, emptyList(), emptyList(), emptyList()));

        // @format:off
        TupleVector vector1 = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
                List.of(VectorTestUtils.vv(Type.Int, 10, 20, 30), VectorTestUtils.vv(Type.String, "ten", "twenty", "thirty åäö")));
        TupleVector vector2 = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
                List.of(VectorTestUtils.vv(Type.Int, 100, 200, 300), VectorTestUtils.vv(Type.String, "hundret", "two hundret", "three hundred")));
        // @format:on

        sink.execute(context, new TupleIterator()
        {
            int index = 0;

            @Override
            public TupleVector next()
            {
                if (index > 2)
                {
                    throw new NoSuchElementException();
                }
                return index == 1 ? vector1
                        : vector2;
            }

            @Override
            public boolean hasNext()
            {
                return index++ <= 1;
            }
        });

        // CSOFF
        assertEquals(
                "[{\"intCol\":10,\"stringCol\":\"ten\"},{\"intCol\":20,\"stringCol\":\"twenty\"},{\"intCol\":30,\"stringCol\":\"thirty åäö\"},{\"intCol\":100,\"stringCol\":\"hundret\"},{\"intCol\":200,\"stringCol\":\"two hundret\"},{\"intCol\":300,\"stringCol\":\"three hundred\"}]",
                FileUtils.readFileToString(tempFile.toFile(), StandardCharsets.UTF_8));
        // CSON
        Files.delete(tempFile);
    }

    @Test
    void test_datasink_insertinto() throws IOException
    {
        CatalogRegistry catalogRegistry = new CatalogRegistry();
        catalogRegistry.registerCatalog("fs", c);
        ExecutionContext context = new ExecutionContext(new QuerySession(catalogRegistry));

        Path tempFile = Files.createTempFile("fsCatalogSink", "txt");
        tempFile = Files.move(tempFile, Path.of(tempFile.toString() + ".txt"));
        Files.deleteIfExists(tempFile);
        QualifiedName tableName = QualifiedName.of(tempFile.toAbsolutePath()
                .toString());
        IDatasink sink = c.getInsertIntoSink(context.getSession(), CATALOG_ALIAS, tableName, new InsertIntoData(0, emptyList(), emptyList(), List.of("newIntCol", "newStringCol")));

        // @format:off
        TupleVector vector1 = TupleVector.of(Schema.of(Column.of("intCol", Type.Int), Column.of("stringCol", Type.String)),
                List.of(VectorTestUtils.vv(Type.Int, 10, 20, 30), VectorTestUtils.vv(Type.String, "ten", "twenty", "thirty åäö")));
        // @format:on

        sink.execute(context, TupleIterator.singleton(vector1));

        // Insert into again but with json
        //@formatter:off
        sink = c.getInsertIntoSink(context.getSession(), CATALOG_ALIAS, tableName, new InsertIntoData(0, emptyList(),
                List.of(
                    new Option(FilesystemCatalog.FORMAT, new LiteralStringExpression("JSON")),
                    new Option(QualifiedName.of("prettyPrint"), LiteralBooleanExpression.TRUE)
                ),
                List.of("newIntCol2", "newStringCol2")));
        //@formatter:on

        sink.execute(context, TupleIterator.singleton(vector1));
        // CSOFF
        assertEquals("""
                newIntCol newStringCol
                10 ten
                20 twenty
                30 thirty åäö
                [ {
                  "newIntCol2" : 10,
                  "newStringCol2" : "ten"
                }, {
                  "newIntCol2" : 20,
                  "newStringCol2" : "twenty"
                }, {
                  "newIntCol2" : 30,
                  "newStringCol2" : "thirty åäö"
                } ]
                """, FileUtils.readFileToString(tempFile.toFile(), StandardCharsets.UTF_8)
                .replaceAll("\\r", "") + "\n");
        // CSON
        Files.delete(tempFile);
    }

    private IExecutionContext mockExecutionContext()
    {
        return mockExecutionContext(emptyMap());
    }

    private IExecutionContext mockExecutionContext(Map<String, Object> properties)
    {
        IExecutionContext context = Mockito.spy(IExecutionContext.class);
        IQuerySession session = Mockito.mock(IQuerySession.class);
        when(session.getSystemCatalog()).thenReturn(systemCatalogMock);
        when(context.getSession()).thenReturn(session);
        when(context.getExpressionFactory()).thenReturn(new ExpressionFactory());

        when(session.getCatalogProperty(eq(CATALOG_ALIAS), anyString(), any())).thenCallRealMethod();
        when(session.getCatalogProperty(eq(CATALOG_ALIAS), anyString())).thenAnswer(new Answer<Object>()
        {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable
            {
                String key = invocation.getArgument(1);
                Object value = properties.get(key);
                return value == null ? ValueVector.literalNull(ResolvedType.ANY, 1)
                        : ValueVector.literalAny(1, value);
            }
        });

        return context;
    }
}
