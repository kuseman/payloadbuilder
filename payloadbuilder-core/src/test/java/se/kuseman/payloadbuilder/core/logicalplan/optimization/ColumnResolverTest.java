package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference.ColumnReference;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.core.expression.VariableExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.MaxRowCountAssert;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.parser.ParseException;

/** Test of {@link ColumnResolver} */
// CSOFF
public class ColumnResolverTest extends ALogicalPlanOptimizerTest
// CSON
{
    private final ColumnResolver optimizer = new ColumnResolver();

    @Test
    public void test_sub_query_expression_with_join_and_outer_references()
    {
        //@formatter:off
        String query =  "SELECT * "
                + "FROM tableA a "
                + "OUTER POPULATE APPLY "
                + "( "
                + "  SELECT b.col1 "
                + "  ,      b.col2 "
                + "  FROM (a.\"table\") t "
                + "  INNER JOIN tableB b "
                + "    ON b.id = t.id "
                + ") t";
        //@formatter:on
        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(3, "", "tableB", "b");
        TableSourceReference a_table = new TableSourceReference(2, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("a.table"), "t");

        TableSourceReference subQueryT = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("t"), "t");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        Schema schemaA_table = Schema.of(ast("t", Type.Any, a_table));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                        tableScan(schemaA, tableA),
                        subQuery(
                            projection(
                                new Join(
                                    new ExpressionScan(a_table, schemaA_table, ocre("table", tableA, ResolvedType.of(Type.Any), CoreColumn.Type.REGULAR), null),
                                    tableScan(schemaB, tableB),
                                    Join.Type.INNER,
                                    null,
                                    eq(cre("id", tableB), cre("id", a_table)),
                                    null,
                                    false,
                                    schemaA),
                                asList(cre("col1", tableB), cre("col2", tableB)),
                                subQueryT),
                        subQueryT),
                        Join.Type.LEFT,
                        "t",
                        null,
                        asSet(col("table", Type.Any, tableA)),
                        false,
                        schemaA),
                    List.of(new AsteriskExpression(QualifiedName.of(), null, Set.of(tableA, tableB))));

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(
                        ast("a", Type.Any, tableA),
                        new CoreColumn("t", ResolvedType.table(Schema.of(
                                col("col1", Type.Any, tableB),
                                col("col2", Type.Any, tableB))), "", false, tableB, CoreColumn.Type.POPULATED)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_expression_scan_schema_less()
    {
        //@formatter:off
        String query = " "
                + "select a.*, x.* "
                + "from tableA a "
                + "cross apply (a.b) x ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference a_b = new TableSourceReference(1, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("a.b"), "x");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaA_b = Schema.of(ast("x", Type.Any, a_b));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                        tableScan(schemaA, tableA),
                        new ExpressionScan(a_b, schemaA_b, ocre("b", tableA), null),
                        Join.Type.INNER,
                        null,
                        null,
                        asSet(col("b", Type.Any, tableA)),
                        false,
                        schemaA),
                    asList(new AsteriskExpression(QualifiedName.of("a"), null, asSet(tableA)),
                            new AsteriskExpression(QualifiedName.of("x"), null, asSet(a_b))));

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(
                        new CoreColumn("", ResolvedType.of(Type.Any), "a.*", false, tableA, CoreColumn.Type.ASTERISK),
                        new CoreColumn("", ResolvedType.of(Type.Any), "x.*", false, a_b, CoreColumn.Type.ASTERISK)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .withStrictTypeChecking()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_expression_scan_schema_full()
    {
        //@formatter:off
        String query = " "
                + "select * "
                + "from stableE e "
                + "cross apply (e.col3) x ";
        //@formatter:on

        session.setCatalogProperty(TEST, STABLE_E_ID, 0);

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference sTableE = sTableE(0);

        //@formatter:off
        Schema schema_sTableE = Schema.of(
                Column.of("nCol1", Type.Int),
                Column.of("nCol2", Type.String));

        TableSourceReference e_col3 = new TableSourceReference(1, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("e.col3"), "x");

        Schema schema_col3 = Schema.of(
                col("nCol1", Type.Int, e_col3),
                col("nCol2", Type.String, e_col3));

        ILogicalPlan expected =
                new Projection(
                        new Join(
                            tableScan(schemaSTableE(0), sTableE),
                            new ExpressionScan(e_col3, schema_col3, ocre("col3", sTableE, 1, ResolvedType.table(schemaSTableE(0).getColumns().get(1).getType().getSchema())), null),
                            Join.Type.INNER,
                            null,
                            null,
                            asSet(col("col3", ResolvedType.table(schemaSTableE(1).getColumns().get(1).getType().getSchema()), sTableE)),
                            false,
                            schemaSTableE(0)),
                        List.of(
                                cre("col1", sTableE, 0, ResolvedType.DOUBLE),
                                cre("col3", sTableE, 1, ResolvedType.table(schema_sTableE)),
                                cre("col6", sTableE, 2, ResolvedType.STRING),
                                cre("nCol1", e_col3, 3, ResolvedType.INT),
                                cre("nCol2", e_col3, 4, ResolvedType.STRING)
                                ),
                    null);
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(
                        col("col1", Type.Double, sTableE),
                        col("col3", ResolvedType.table(schema_col3), sTableE),
                        col("col6", Type.String, sTableE),
                        col("nCol1", Type.Int, e_col3),
                        col("nCol2", Type.String, e_col3)
                        ));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .usingOverriddenEquals()
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_expression_scan_table_variable_with_provided_schema()
    {
        //@formatter:off
        String query = " "
                + "select * "
                + "from @tbl e ";
        //@formatter:on

        //@formatter:off
        context.setVariable("tbl", ValueVector.literalTable(TupleVector.of(Schema.of(
                Column.of("col1", Type.Int),
                Column.of("col2", Type.Boolean)
                ), List.of(
                ValueVector.empty(ResolvedType.of(Type.Int)),
                ValueVector.empty(ResolvedType.of(Type.Boolean))
                ))));
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference expectedTable = new TableSourceReference(0, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("tbl"), "e");

        Schema expectedSchema = Schema.of(col("col1", ResolvedType.of(Type.Int), expectedTable), col("col2", ResolvedType.of(Type.Boolean), expectedTable));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new ExpressionScan(
                        expectedTable,
                        expectedSchema, 
                        new VariableExpression("tbl"),
                        null),
                    List.of(cre("col1", expectedTable, 0, ResolvedType.INT),
                            cre("col2", expectedTable, 1, ResolvedType.BOOLEAN)
                            ));

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expectedSchema);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_expression_scan_schema_full_wrong_type()
    {
        //@formatter:off
        String query = " "
                + "select * "
                + "from stableE e "
                + "cross apply (e.col1) x ";
        //@formatter:on

        session.setCatalogProperty(TEST, STABLE_E_ID, 0);

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        try
        {
            optimize(context, plan);
            fail("Should fail because of wrong expressions scan type");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Expression scans must reference either Any or Table types."));
        }
    }

    /**
     * When only having a single schema as input (either current or outer) and we have a multi qualifier (where the first part is NOT an alias) then we can resolve that expression to the single schema
     */
    @Test
    public void test_multi_part_qualifier_gets_resolved_to_single_schema()
    {
        //@formatter:off
        String query = " "
                + "select multi.part.qualifier "
                + "from tableA a";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    tableScan(schemaA, tableA),
                    asList(DereferenceExpression.create(cre("multi", tableA), QualifiedName.of(asList("part", "qualifier")), null)));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(col("qualifier", ResolvedType.of(Type.Any), tableA)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    /**
     * When only having a single schema as input (either current or outer) and we have a multi qualifier (where the first part is NOT an alias) then we can resolve that expression to the single schema
     */
    @Test
    public void test_multi_part_qualifier_gets_resolved_to_single_schema_outer()
    {
        //@formatter:off
        String query = " "
                + "select ( select multi.part.qualifier ) val "
                + "from tableA a";
        //@formatter:on

        ILogicalPlan plan = getPlanBeforeColumnResolver(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    tableScan(schemaA, tableA),
                    asList(new AliasExpression(DereferenceExpression.create(cre("multi", tableA), QualifiedName.of(asList("part", "qualifier")), null), "val"))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(col("val", ResolvedType.of(Type.Any), tableA)));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_distinct_asterisk_expansion()
    {
        //@formatter:off
        String query = " "
                + "select distinct * "
                + "from tableA a";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));

        //@formatter:off
        ILogicalPlan expected =
                new Aggregate(
                    new Projection(
                        tableScan(schemaA, tableA),
                        List.of(new AsteriskExpression(QualifiedName.of(), null, Set.of(tableA))),
                        null),
                    emptyList(),
                    emptyList(),
                    null);
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(ast("a", Type.Any, tableA)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_multi_part_qualifier()
    {
        //@formatter:off
        String query = " "
                + "select a.log.nested.level "
                + "from tableA a";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    tableScan(schemaA, tableA),
                    asList(
                            new DereferenceExpression(
                                new DereferenceExpression(cre("log", tableA), "nested", -1, ResolvedType.of(Type.Any)),
                                "level", -1, ResolvedType.of(Type.Any))),
                    null);
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(col("level", Type.Any, tableA)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_expand_outer_reference_asterisk_in_sub_query_expression()
    {
        //@formatter:off
        String query = " "
                + "select (select b.* for object_array) obj "
                + "from tableA a "
                + "inner populate join tableB b "
                + "  on b.col1 = a.col1 ";
        //@formatter:on

        ILogicalPlan plan = getPlanBeforeColumnResolver(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(1, "", "tableB", "b");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        Schema objectArraySchema = Schema.of(new CoreColumn("", ResolvedType.of(Type.Any), "b.*", false, tableB, CoreColumn.Type.ASTERISK));

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    new Join(
                        new Join(
                            tableScan(schemaA, tableA),
                            tableScan(schemaB, tableB),
                            Join.Type.INNER,
                            "b",
                            eq(cre("col1", tableB), cre("col1", tableA)),
                            null,
                            false,
                            Schema.EMPTY),
                        new OperatorFunctionScan(
                            Schema.of(col("__expr0", ResolvedType.table(Schema.of(ast("", "b.*", Type.Any, tableB))), null, true)),
                            projection(
                                ConstantScan.ONE_ROW_EMPTY_SCHEMA,
                                List.of(new AsteriskExpression(QualifiedName.of("b"), null, Set.of(tableB)))),
                            "",
                            "object_array",
                            null),
                    Join.Type.LEFT,
                    null,
                    (IExpression) null,
                    Set.of(ast("b", ResolvedType.table(schemaB), tableB)),
                    false,
                    Schema.of(ast("a", Type.Any, tableA), pop("b", ResolvedType.table(schemaB), tableB))),
                    asList(new AliasExpression(ce("__expr0", ResolvedType.table(Schema.of(ast("", "b.*", Type.Any, tableB)))), "obj")),
                    null
                );
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(col("obj", ResolvedType.table(objectArraySchema), null)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_aggregate()
    {
        //@formatter:off
        String query = " "
                + "select col, count(col2), col3 "
                + "from tableA a "
                + "group by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));

        //@formatter:off
        ILogicalPlan expected = new Aggregate(
                tableScan(schemaA, tableA),
                asList(cre("col", tableA)),
                asList(
                    agg(cre("col", tableA), true),
                    new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(cre("col2", tableA))),
                    agg(cre("col3", tableA), false)),
                null);
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                col("col", ResolvedType.of(Type.Any), tableA),
                new CoreColumn("", ResolvedType.of(Type.Int), "count(a.col2)", false, CoreColumn.Type.REGULAR),
                col("col3", ResolvedType.array(Type.Any), tableA)
                ));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_lambda_expression_no_populate_schema_less()
    {
        // Test lambdas which not alter the schema for the lambda expression
        // Here we are sill in the outer schema when the lambda expression will evaluate
        // This is typical a list/array etc. that we run a map against (unknown value during planning)
        // x.value =>
        //@formatter:off
        String query = " "
                + "select d.map(x -> x) , "                   // d is a column on tableA, resolve the left dereference itself
                + "       d.map(x -> x.value), "              // d is a column on tableA, dereference the side with "value"
                + "       a.map(y -> y), "                    // since this is not a populated join a is also a column on tableA
                + "       a.map(z -> z.column), "
                + "       col.map(zz -> zz.column + a.value) "  // Access outer scope within lambda
                + "from tableA a ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    tableScan(schemaA, tableA),
                    asList(new FunctionCallExpression(
                            "sys",
                            SystemCatalog.get().getScalarFunction("map"),
                            null,
                            asList(
                               cre("d", tableA),
                               new LambdaExpression(asList("x"),
                                       lce("x", 0),
                                       new int[] {0})
                            )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre("d", tableA),
                                   new LambdaExpression(asList("x"),
                                           new DereferenceExpression(lce("x", 0), "value", -1, ResolvedType.of(Type.Any)),
                                           new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre("a", tableA),
                                   new LambdaExpression(asList("y"),
                                           lce("y", 0),
                                           new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre("a", tableA),
                                   new LambdaExpression(asList("z"),
                                           new DereferenceExpression(lce("z", 0), "column", -1, ResolvedType.of(Type.Any)),
                                           new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre("col", tableA),
                                   new LambdaExpression(asList("zz"),
                                           add(new DereferenceExpression(lce("zz", 0), "column", -1, ResolvedType.of(Type.Any)), cre("value", tableA)),
                                           new int[] {0})
                                ))
                            ));
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
        .usingRecursiveComparison()
        .ignoringFieldsOfTypes(Location.class, Random.class)
        .isEqualTo(Schema.of(
                new CoreColumn("", ResolvedType.of(Type.Any), "map(a.d, x -> x)", false, CoreColumn.Type.REGULAR),
                new CoreColumn("", ResolvedType.of(Type.Any), "map(a.d, x -> x.value)", false, CoreColumn.Type.REGULAR),
                new CoreColumn("", ResolvedType.of(Type.Any), "map(a.a, y -> y)", false, CoreColumn.Type.REGULAR),
                new CoreColumn("", ResolvedType.of(Type.Any), "map(a.a, z -> z.column)", false, CoreColumn.Type.REGULAR),
                new CoreColumn("", ResolvedType.of(Type.Any), "map(a.col, zz -> zz.column + a.value)", false, CoreColumn.Type.REGULAR)
                ));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_lambda_expression_populate_schema_less()
    {
        //@formatter:off
        String query = " "
                + "select b.map(x -> x) , "
                + "       b.map(x -> x.col), "
                + "       b.map(z -> z.column), "
                + "       b.map(zz -> zz.col2 + a.value) "  
                + "from tableA a "
                + "inner populate join tableB b"
                + " on b.col = a.col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(1, "", "tableB", "b");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                            tableScan(schemaA, tableA),
                            tableScan(schemaB, tableB),
                            Join.Type.INNER,
                            "b",
                            eq(cre("col", tableB), cre("col", tableA)),
                            asSet(),
                            false,
                            Schema.EMPTY),
                    asList(new FunctionCallExpression(
                            "sys",
                            SystemCatalog.get().getScalarFunction("map"),
                            null,
                            asList(
                               cre("b", tableB, ResolvedType.table(schemaB), CoreColumn.Type.POPULATED),
                               new LambdaExpression(asList("x"),
                                       lce("x", 0, ResolvedType.object(schemaB)),
                                       new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre("b", tableB, ResolvedType.table(schemaB), CoreColumn.Type.POPULATED),
                                   new LambdaExpression(asList("x"),
                                           DereferenceExpression.create(lce("x", 0, ResolvedType.object(schemaB)), QualifiedName.of("col"), null),
                                           new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                  cre("b", tableB, ResolvedType.table(schemaB), CoreColumn.Type.POPULATED),
                                   new LambdaExpression(asList("z"),
                                           DereferenceExpression.create(lce("z", 0, ResolvedType.object(schemaB)), QualifiedName.of("column"), null),
                                           new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre("b", tableB, ResolvedType.table(schemaB), CoreColumn.Type.POPULATED),
                                   new LambdaExpression(asList("zz"),
                                           add(
                                               DereferenceExpression.create(lce("zz", 0, ResolvedType.object(schemaB)), QualifiedName.of("col2"), null),
                                               cre("value", tableA)),
                                           new int[] {0})
                                ))
                            ));
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .isEqualTo(Schema.of(
                    new CoreColumn("", ResolvedType.array(ResolvedType.object(schemaB)), "map(b.b, x -> x)", false, CoreColumn.Type.REGULAR),       // ValueVector of ObjectVectors
                    new CoreColumn("", ResolvedType.array(ResolvedType.of(Type.Any)), "map(b.b, x -> x.col)", false, CoreColumn.Type.REGULAR),
                    new CoreColumn("", ResolvedType.array(ResolvedType.of(Type.Any)), "map(b.b, z -> z.column)", false, CoreColumn.Type.REGULAR),
                    new CoreColumn("", ResolvedType.array(ResolvedType.of(Type.Any)), "map(b.b, zz -> zz.col2 + a.value)", false, CoreColumn.Type.REGULAR)
                    ));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_sub_query_expression_missing_alias()
    {
        //@formatter:off
        String query = "select (select d.data row_id, s.col1, s.col2 from source for object_array) obj "
        + "from data d "
        +"where id = 1";
        //@formatter:on

        ILogicalPlan plan = getPlanBeforeColumnResolver(query);

        try
        {
            optimize(context, plan);
            fail("Should fail with unkown column");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("s.col1 cannot be bound"));
        }
    }

    @Test
    public void test_correlated_sub_query_correlates_computed_column_from_other_subquery()
    {
        //@formatter:off
        String query = ""
        + "select * "
        + "from "
        + "( "
        + "    select a.col + a.col2 col "
        + "    from tableA a "
        + ") x "
        + "cross apply "
        + "( "
        + "    select * "
        + "    from tableA a "
        + "    where x.col > a.col "
        + ") y ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA0 = of(1, "", "tableA", "a");
        TableSourceReference tableA1 = of(3, "", "tableA", "a");
        TableSourceReference subQueryX = new TableSourceReference(0, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        TableSourceReference subQueryY = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("y"), "y");

        Schema schemaA0 = Schema.of(ast("a", Type.Any, tableA0));
        Schema schemaA1 = Schema.of(ast("a", Type.Any, tableA1));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                        subQuery(
                            projection(
                                tableScan(schemaA0, tableA0),
                                asList(new AliasExpression(add(cre("col", tableA0), cre("col2", tableA0)), "col")),
                                subQueryX),
                            subQueryX),
                        subQuery(
                            projection(
                                new Filter(
                                    tableScan(schemaA1, tableA1),
                                    null,
                                    gt(ocre("col", subQueryX, 0, ResolvedType.of(Type.Any), CoreColumn.Type.REGULAR), cre("col", tableA1))),
                                List.of(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(tableA1))),
                                subQueryY),
                            subQueryY),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(col("col", Type.Any, subQueryX)),
                        false,
                        Schema.of(col("col", Type.Any, subQueryX))),
                    List.of(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(tableA1, subQueryX))),
                    null);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(col("col", Type.Any, subQueryX), ast("a", Type.Any, tableA1)));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_precedence_of_current_outer_matches_asterisk_and_not_asterisk()
    {
        //@formatter:off
        String query = ""
        + "select (select col2,"                        // Unqualified => b
        + "               c.col3,"                      // Outer scope to populated join
        + "               b.col4,"                      // Qualified to over alias (b)
        + "               a.col3, "                     // Outer scope to non populated join
        + "               col6, "                       // Non asterisk outer column (d)
        + "               c "                           // Here we should have a match on a populated column (c) in outer scope
                                                        // which has higher precedence than an asterisk column in current scope (b)
        + "               from (b) b "
        + "               for object_array) \"values\" "
        + "from tableA a "
        + "inner populate join tableB b "
        + "  on b.col1 = a.col1 "
        + "inner populate join tableC c "
        + "  on c.col1 = a.col1 "
        + "inner join stableD d "
        + "  on d.col1 = a.col1 "
        + "";
        //@formatter:on

        session.setCatalogProperty(TEST, STABLE_D_ID, 3);

        ILogicalPlan plan = getPlanBeforeColumnResolver(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(1, "", "tableB", "b");
        TableSourceReference tableC = of(2, "", "tableC", "c");
        TableSourceReference sTableD = sTableD(3);

        TableSourceReference e_b = new TableSourceReference(4, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("b"), "b");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        Schema schemaC = Schema.of(ast("c", Type.Any, tableC));

        Schema schemaSTableD = schemaSTableD(3);

        //@formatter:off
        Schema objectArraySchema = Schema.of(
            col("col2", ResolvedType.of(Type.Any), e_b.withParent(tableB)),
            col("col3", ResolvedType.array(Type.Any), tableC),
            col("col4", ResolvedType.of(Type.Any), e_b.withParent(tableB)),
            col("col3", ResolvedType.of(Type.Any), tableA),
            col("col6", ResolvedType.of(Type.String), sTableD),
            pop("c", ResolvedType.table(schemaC), tableC)
        );
        
        ILogicalPlan expected = 
                projection(
                    new Join(
                        new Join( 
                            new Join(
                               new Join(
                                   tableScan(schemaA, tableA),
                                   tableScan(schemaB, tableB),
                                   Join.Type.INNER,
                                   "b",
                                   eq(cre("col1", tableB), cre("col1", tableA)),
                                   null,
                                   false,
                                   Schema.EMPTY),
                               tableScan(schemaC, tableC),
                               Join.Type.INNER,
                               "c",
                               eq(cre("col1", tableC), cre("col1", tableA)),
                               null,
                               false,
                               Schema.EMPTY),
                            tableScan(schemaSTableD, sTableD),
                            Join.Type.INNER,
                            null,
                            eq(cre("col1", sTableD, ResolvedType.of(Type.Double), CoreColumn.Type.REGULAR), cre("col1", tableA)),
                            null,
                            false,
                            Schema.EMPTY),
                        new OperatorFunctionScan(
                                Schema.of(col("__expr0", ResolvedType.table(objectArraySchema), null, true)),
                                projection(
                                    new ExpressionScan(
                                        e_b.withParent(tableB),
                                        Schema.of(ast("b", ResolvedType.of(Type.Any), e_b.withParent(tableB))),
                                        ocre("b", tableB, ResolvedType.table(schemaB), CoreColumn.Type.POPULATED),
                                        null),
                                    asList(
                                        cre("col2", e_b.withParent(tableB), tableB),
                                        new DereferenceExpression(ocre("c", tableC, "c", ResolvedType.table(schemaC), CoreColumn.Type.POPULATED),
                                                "col3", -1, ResolvedType.array(Type.Any),
                                                new ColumnReference(tableC, CoreColumn.Type.REGULAR)),
                                        cre("col4", e_b.withParent(tableB), tableB),
                                        ocre("col3", tableA),
                                        ocre("col6", sTableD, "col6", ResolvedType.of(Type.String), CoreColumn.Type.REGULAR),
                                        ocre("c", tableC, "c", ResolvedType.table(schemaC), CoreColumn.Type.POPULATED)
                                    )),
                                "",
                                "object_array",
                                null),
                        Join.Type.LEFT,
                        null,
                        null,
                        asSet(
                            pop("b", ResolvedType.table(schemaB), tableB),
                            pop("col3", ResolvedType.table(schemaC), tableC),
                            col("col3", ResolvedType.of(Type.Any), tableA),
                            col("col6", ResolvedType.of(Type.String), sTableD),     // static schema col6 was chosen instead of asterisk
                            pop("c", ResolvedType.table(schemaC), tableC)
                        ),
                        false,
                        Schema.of(
                            ast("a", ResolvedType.ANY, tableA),
                            pop("b", ResolvedType.table(schemaB), tableB),
                            pop("c", ResolvedType.table(schemaC), tableC),
                            col("col1", ResolvedType.DOUBLE, sTableD),
                            col("col6", ResolvedType.STRING, sTableD)
                        )),
                    asList(new AliasExpression(ce("__expr0", ResolvedType.table(objectArraySchema)), "values")));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(Schema.of(col("values", ResolvedType.table(objectArraySchema), null)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_function_scan_with_correlated_arguments()
    {
        //@formatter:off
        String query = "select a.col, r.* "
                + "from tableA a "
                + "cross apply range(1, a.col2) r ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference range = new TableSourceReference(1, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaRange = Schema.of(col("Value", Type.Int, range));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    new TableFunctionScan(range, schemaRange, asList(intLit(1), ocre("col2", tableA)), emptyList(), null),
                    Join.Type.INNER,
                    null,
                    (IExpression) null,
                    asSet(col("col2", Type.Any, tableA)),
                    false,
                    schemaA),
                asList(cre("col", tableA), cre("Value", range, ResolvedType.of(Type.Int), CoreColumn.Type.REGULAR))
                );
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(Schema.of(col("col", Type.Any, tableA), col("Value", Type.Int, range)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_function_scan_with_non_correlated_arguments()
    {
        //@formatter:off
        String query = "select a.col, r.* "
                + "from tableA a "
                + "cross apply range(1, 10) r ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference range = new TableSourceReference(1, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaRange = Schema.of(col("Value", Type.Int, range));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    new TableFunctionScan(range, schemaRange, asList(intLit(1), intLit(10)), emptyList(), null),
                    Join.Type.INNER,
                    null,
                    (IExpression) null,
                    asSet(),
                    false,
                    schemaA),
                asList(cre("col", tableA), cre("Value", range, ResolvedType.of(Type.Int), CoreColumn.Type.REGULAR))
                );
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(Schema.of(col("col", Type.Any, tableA), col("Value", Type.Int, range)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);
    }

    @Test
    public void test_function_scan_with_non_existing_argument()
    {
        //@formatter:off
        String query = "select * "
                + "from  range(1, a.col) r ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        try
        {
            optimize(context, plan);
            fail("Should fail cause of unbound column");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("a.col cannot be bound"));
        }
    }

    @Test
    public void test_nested_correlated_function_scans()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from range(1,10) a "
                + "cross apply range (1, a.Value) r1 "
                + "cross apply range (1, r1.Value) r2";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference rangeA = new TableSourceReference(0, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "a");
        TableSourceReference rangeR1 = new TableSourceReference(1, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r1");
        TableSourceReference rangeR2 = new TableSourceReference(2, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r2");

        Schema schemaRangeA = Schema.of(col("Value", Type.Int, rangeA));
        Schema schemaRangeR1 = Schema.of(col("Value", Type.Int, rangeR1));
        Schema schemaRangeR2 = Schema.of(col("Value", Type.Int, rangeR2));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                            new Join(
                                new TableFunctionScan(rangeA, schemaRangeA, asList(intLit(1), intLit(10)), emptyList(), null),
                                new TableFunctionScan(rangeR1, schemaRangeR1, asList(intLit(1), ocre("Value", rangeA, 0, ResolvedType.of(Type.Int))), emptyList(), null),
                                Join.Type.INNER,
                                null,
                                (IExpression) null,
                                asSet(col("Value", Type.Int, rangeA)),
                                false,
                                schemaRangeA),
                            new TableFunctionScan(rangeR2, schemaRangeR2, asList(intLit(1), ocre("Value", rangeR1, 1, ResolvedType.of(Type.Int))), emptyList(), null),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(col("Value", Type.Int, rangeR1)),
                        false,
                        SchemaUtils.joinSchema(schemaRangeA, schemaRangeR1)),
                    List.of(
                            cre("Value", rangeA, 0, ResolvedType.INT),
                            cre("Value", rangeR1, 1, ResolvedType.INT),
                            cre("Value", rangeR2, 2, ResolvedType.INT)
                            ));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(SchemaUtils.joinSchema(schemaRangeA, SchemaUtils.joinSchema(schemaRangeR1, schemaRangeR2)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_correlated_function_scans_qualified_asterisk()
    {
        //@formatter:off
        String query = ""
                + "select r1.* "
                + "from range(1,10) a "
                + "cross apply range (1, a.Value) r1 "
                + "cross apply range (1, r1.Value) r2";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference rangeA = new TableSourceReference(0, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "a");
        TableSourceReference rangeR1 = new TableSourceReference(1, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r1");
        TableSourceReference rangeR2 = new TableSourceReference(2, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r2");

        Schema schemaRangeA = Schema.of(col("Value", Type.Int, rangeA));
        Schema schemaRangeR1 = Schema.of(col("Value", Type.Int, rangeR1));
        Schema schemaRangeR2 = Schema.of(col("Value", Type.Int, rangeR2));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                            new Join(
                                new TableFunctionScan(rangeA, schemaRangeA, asList(intLit(1), intLit(10)), emptyList(), null),
                                new TableFunctionScan(rangeR1, schemaRangeR1, asList(intLit(1), ocre("Value", rangeA, 0, ResolvedType.of(Type.Int))), emptyList(), null),
                                Join.Type.INNER,
                                null,
                                (IExpression) null,
                                asSet(col("Value", Type.Int, rangeA)),
                                false,
                                schemaRangeA),
                            new TableFunctionScan(rangeR2, schemaRangeR2, asList(intLit(1), ocre("Value", rangeR1, 1, ResolvedType.of(Type.Int))), emptyList(), null),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(col("Value", Type.Int, rangeR1)),
                        false,
                        SchemaUtils.joinSchema(schemaRangeA, schemaRangeR1)),
                asList(cre("Value", rangeR1, 1, ResolvedType.of(Type.Int))));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(schemaRangeR1);

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_correlated_function_scans_populate_asterisk()
    {
        //@formatter:off
        String query = ""
                + "select r1.* "
                + "from range(1,10) a "
                + "cross populate apply range (1, a.Value) r1 "
                + "cross apply range (1, r1.Value) r2";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference rangeA = new TableSourceReference(0, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "a");
        TableSourceReference rangeR1 = new TableSourceReference(1, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r1");
        TableSourceReference rangeR2 = new TableSourceReference(2, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r2");

        Schema schemaRangeA = Schema.of(col("Value", Type.Int, rangeA));
        Schema schemaRangeR1 = Schema.of(col("Value", Type.Int, rangeR1));
        Schema schemaRangeR2 = Schema.of(col("Value", Type.Int, rangeR2));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                            new Join(
                                new TableFunctionScan(rangeA, schemaRangeA, asList(intLit(1), intLit(10)), emptyList(), null),
                                new TableFunctionScan(rangeR1, schemaRangeR1, asList(intLit(1), ocre("Value", rangeA, 0, ResolvedType.of(Type.Int))), emptyList(), null),
                                Join.Type.INNER,
                                "r1",
                                (IExpression) null,
                                asSet(col("Value", Type.Int, rangeA)),
                                false,
                                schemaRangeA),
                            new TableFunctionScan(rangeR2, schemaRangeR2, asList(
                                    intLit(1),
                                    new DereferenceExpression(ocre("r1", rangeR1, 1, ResolvedType.table(schemaRangeR1), CoreColumn.Type.POPULATED),
                                            "Value", 0, ResolvedType.array(ResolvedType.of(Type.Int)),
                                            new ColumnReference(rangeR1, CoreColumn.Type.REGULAR))),
                                    emptyList(), null),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(pop("Value", ResolvedType.table(schemaRangeR1), rangeR1)),
                        false,
                        SchemaUtils.joinSchema(schemaRangeA, schemaRangeR1, "r1")),
                    asList(new DereferenceExpression(
                                cre("r1", rangeR1, 1, ResolvedType.table(schemaRangeR1), CoreColumn.Type.POPULATED),
                                "Value",
                                0,
                                ResolvedType.array(ResolvedType.of(Type.Int)),
                            new ColumnReference(rangeR1, CoreColumn.Type.REGULAR))
                    ));

        assertEquals(
                Schema.of(
                        col("Value", ResolvedType.array(ResolvedType.of(Type.Int)), rangeR1)),
                actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_correlated_function_scans_populate()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from range(1,10) a "
                + "cross populate apply range (1, a.Value) r1 "
                + "cross apply range (1, r1.Value) r2";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference rangeA = new TableSourceReference(0, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "a");
        TableSourceReference rangeR1 = new TableSourceReference(1, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r1");
        TableSourceReference rangeR2 = new TableSourceReference(2, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r2");

        Schema schemaRangeA = Schema.of(col("Value", Type.Int, rangeA));
        Schema schemaRangeR1 = Schema.of(col("Value", Type.Int, rangeR1));
        Schema schemaRangeR2 = Schema.of(col("Value", Type.Int, rangeR2));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                            new Join(
                                new TableFunctionScan(rangeA, schemaRangeA, asList(intLit(1), intLit(10)), emptyList(), null),
                                new TableFunctionScan(rangeR1, schemaRangeR1, asList(intLit(1), ocre("Value", rangeA, 0, ResolvedType.of(Type.Int))), emptyList(), null),
                                Join.Type.INNER,
                                "r1",
                                (IExpression) null,
                                asSet(col("Value", Type.Int, rangeA)),
                                false,
                                schemaRangeA),
                            new TableFunctionScan(rangeR2, schemaRangeR2, asList(
                                    intLit(1),
                                    new DereferenceExpression(ocre("r1", rangeR1, 1, ResolvedType.table(schemaRangeR1), CoreColumn.Type.POPULATED),
                                            "Value", 0, ResolvedType.array(ResolvedType.of(Type.Int)),
                                            new ColumnReference(rangeR1, CoreColumn.Type.REGULAR))),
                                    emptyList(), null),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(pop("Value", ResolvedType.table(schemaRangeR1), rangeR1)),
                        false,
                        SchemaUtils.joinSchema(schemaRangeA, schemaRangeR1, "r1")),
                    List.of(
                            cre("Value", rangeA, 0, ResolvedType.INT),
                            cre("r1", rangeR1, 1, ResolvedType.table(schemaRangeR1), CoreColumn.Type.POPULATED),
                            cre("Value", rangeR2, 2, ResolvedType.INT)
                            ));
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class)
            .isEqualTo(Schema.of(
                // All a
                schemaRangeA.getColumns().get(0),
                // Populate r1
                new CoreColumn("r1", ResolvedType.table(schemaRangeR1), "", false, rangeR1, CoreColumn.Type.POPULATED),
                // All r2
                schemaRangeR2.getColumns().get(0)
                ));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_correlated_function_scans_double_populate()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from range(1,10) a "
                + "cross populate apply range (1, a.Value) r1 "
                + "cross populate apply range (1, r1.Value) r2";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference rangeA = new TableSourceReference(0, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "a");
        TableSourceReference rangeR1 = new TableSourceReference(1, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r1");
        TableSourceReference rangeR2 = new TableSourceReference(2, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "r2");

        Schema schemaRangeA = Schema.of(col("Value", Type.Int, rangeA));
        Schema schemaRangeR1 = Schema.of(col("Value", Type.Int, rangeR1));
        Schema schemaRangeR2 = Schema.of(col("Value", Type.Int, rangeR2));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                            new Join(
                                new TableFunctionScan(rangeA, schemaRangeA, asList(intLit(1), intLit(10)), emptyList(), null),
                                new TableFunctionScan(rangeR1, schemaRangeR1, asList(intLit(1), ocre("Value", rangeA, 0, ResolvedType.of(Type.Int))), emptyList(), null),
                                Join.Type.INNER,
                                "r1",
                                (IExpression) null,
                                asSet(col("Value", Type.Int, rangeA)),
                                false,
                                schemaRangeA),
                            new TableFunctionScan(rangeR2, schemaRangeR2, asList(
                                    intLit(1),
                                    new DereferenceExpression(ocre("r1", rangeR1, 1, ResolvedType.table(schemaRangeR1), CoreColumn.Type.POPULATED),
                                            "Value", 0, ResolvedType.array(ResolvedType.of(Type.Int)),
                                            new ColumnReference(rangeR1, CoreColumn.Type.REGULAR))),
                                    emptyList(), null),
                        Join.Type.INNER,
                        "r2",
                        (IExpression) null,
                        asSet(pop("Value", ResolvedType.table(schemaRangeR1), rangeR1)),
                        false,
                        SchemaUtils.joinSchema(schemaRangeA, schemaRangeR1, "r1")),
                    List.of(
                            cre("Value", rangeA, 0, ResolvedType.INT),
                            cre("r1", rangeR1, 1, ResolvedType.table(schemaRangeR1), CoreColumn.Type.POPULATED),
                            cre("r2", rangeR2, 2, ResolvedType.table(schemaRangeR2), CoreColumn.Type.POPULATED)
                            ));
        //@formatter:on

        //@formatter:off
        assertEquals(Schema.of(
                // All a
                schemaRangeA.getColumns().get(0),
                // Populate r1
                new CoreColumn("r1", ResolvedType.table(schemaRangeR1), "", false, rangeR1, CoreColumn.Type.POPULATED),
                // Populate r2
                new CoreColumn("r2", ResolvedType.table(schemaRangeR2), "", false, rangeR2, CoreColumn.Type.POPULATED)
                ), actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_populate_join_schema_less()
    {
        //@formatter:off
        String query = ""
                + "select a.col, b.col "                // b.col is inside a populated schema
                + "from tableA a "
                + "inner populate join tableB b "
                + "  on b.col2 = a.col2 ";
        //@formatter:on

        // b.col => ColumnReference expression with ordinal/column b and path col

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(1, "", "tableB", "b");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    tableScan(schemaB, tableB),
                    Join.Type.INNER,
                    "b",
                    eq(cre("col2", tableB), cre("col2", tableA)),
                    asSet(),
                    false,
                    Schema.EMPTY),
                asList(cre("col", tableA), new DereferenceExpression(cre("b", tableB, "b",
                        ResolvedType.table(schemaB), CoreColumn.Type.POPULATED), "col", -1, ResolvedType.array(Type.Any),
                        new ColumnReference(tableB, CoreColumn.Type.REGULAR)))
                );
        //@formatter:on

        //@formatter:off

        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class)
            .isEqualTo(Schema.of(
                col("col", Type.Any, tableA),
                new CoreColumn("col", ResolvedType.array(ResolvedType.of(Type.Any)), "", false, tableB, CoreColumn.Type.REGULAR)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_populate_join_schema_less_populate_alias_projection()
    {
        //@formatter:off
        String query = ""
                + "select a.col, b "                // b is of tuple vector type
                + "from tableA a "
                + "inner populate join tableB b "
                + "  on b.col2 = a.col2 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(1, "", "tableB", "b");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    tableScan(schemaB, tableB),
                    Join.Type.INNER,
                    "b",
                    eq(cre("col2", tableB), cre("col2", tableA)),
                    asSet(),
                    false,
                    Schema.EMPTY),
                asList(cre("col", tableA), cre("b", tableB, ResolvedType.table(schemaB), CoreColumn.Type.POPULATED))
                );
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(col("col", Type.Any, tableA), pop("b", ResolvedType.table(schemaB), tableB)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_populate_join_schema_less_asterisk_select()
    {
        //@formatter:off
        String query = ""
                + "select b.*  "
                + "from tableA a "
                + "inner populate join tableB b "
                + "  on b.col2 = a.col2 ";
        //@formatter:on

        // b.col => ColumnReference expression with ordinal/column b and path col

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(1, "", "tableB", "b");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    tableScan(schemaB, tableB),
                    Join.Type.INNER,
                    "b",
                    eq(cre("col2", tableB), cre("col2", tableA)),
                    asSet(),
                    false,
                    Schema.EMPTY),
                asList(new AsteriskExpression(QualifiedName.of("b"), null, Set.of(tableB)))
                );
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(new CoreColumn("", ResolvedType.of(Type.Any), "b.*", false, tableB, CoreColumn.Type.ASTERISK)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_expression_scan_on_root_missing_table_column()
    {
        String query = "select col from (a) a for object_array";

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        try
        {
            optimize(context, plan);
            fail("Should fail becuase of a can not be bound");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("a cannot be bound"));
        }
    }

    @Test
    public void test_populate_join_expression_scan_schema_less()
    {
        //@formatter:off
        // Here we are accessing what seems to be outer columns (b.col, b.col2 in sub query expression) but we have an for clause
        // here which makes this a plain column access since we are going to scan the tuple vector in alias b as 
        // plain table source
        String query = ""
                + "select a.col, b.col bOuterCol, ( select b.col, b.col2, a.col3 from (b) b for object_array ) "
                + "from tableA a "
                + "inner populate join tableB b "
                + "  on b.col2 = a.col2 ";
        //@formatter:on

        ILogicalPlan plan = getPlanBeforeColumnResolver(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(1, "", "tableB", "b");
        TableSourceReference e_b = new TableSourceReference(2, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("b"), "b");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        Schema subQuerySchema = Schema.of(col("col", ResolvedType.ANY, e_b.withParent(tableB)), col("col2", ResolvedType.ANY, e_b.withParent(tableB)), col("col3", ResolvedType.ANY, tableA));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    new Join(
                        tableScan(schemaA, tableA),
                        tableScan(schemaB, tableB),
                        Join.Type.INNER,
                        "b",
                        eq(cre("col2", tableB), cre("col2", tableA)),
                        asSet(),
                        false,
                        Schema.EMPTY),
                    new OperatorFunctionScan(
                        Schema.of(col("__expr0", ResolvedType.table(subQuerySchema), null, true)),
                        projection(
                            new ExpressionScan(
                                e_b.withParent(tableB),
                                Schema.of(ast("b", ResolvedType.ANY, e_b.withParent(tableB))),
                                ocre("b", tableB, ResolvedType.table(schemaB), CoreColumn.Type.POPULATED),
                                null),
                            List.of(
                                cre("col", e_b.withParent(tableB), tableB),
                                cre("col2", e_b.withParent(tableB), tableB),
                                ocre("col3", tableA))
                        ),
                        "",
                        "object_array",
                        null),
                    Join.Type.LEFT,
                    null,
                    (IExpression) null,
                    Set.of(
                        pop("b", ResolvedType.table(schemaB), tableB),
                        col("col3", ResolvedType.ANY, tableA)
                    ),
                    false,
                    Schema.of(
                        ast("a", ResolvedType.ANY, tableA),
                        pop("b", ResolvedType.table(schemaB), tableB)
                    )),
                asList(cre("col", tableA), new AliasExpression(
                        new DereferenceExpression(cre("b", tableB, "b", ResolvedType.table(schemaB), CoreColumn.Type.POPULATED), "col", -1, ResolvedType.array(Type.Any),
                                new ColumnReference(tableB, CoreColumn.Type.REGULAR)), "bOuterCol"),
                        ce("__expr0", ResolvedType.table(subQuerySchema))
                ));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_populate_join_with_expression_scan_schema_full()
    {
        TableSourceReference e_b = new TableSourceReference(2, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("b"), "b");

        //@formatter:off
        String query = """
                select a.col1, b.col2 bOuterCol, ( select b.col3, b.col2, a.col3 from (b) b for object_array )
                from stableA a
                inner populate join stableB b
                  on b.col2 = a.col2
                """;
        //@formatter:off

        ILogicalPlan actual = getPlanBeforeColumnResolver(query);
        actual = optimize(context, actual);

        //@formatter:off
        Schema objectArraySchema = Schema.of(
            col("col3", ResolvedType.of(Type.Float), e_b.withParent(sTableB)),
            col("col2", ResolvedType.of(Type.String), e_b.withParent(sTableB)),
            col("col3", ResolvedType.of(Type.Float), sTableA)
        );
        
        ILogicalPlan expected =
                projection(
                    new Join(
                        new Join(
                            tableScan(schemaSTableA, sTableA),
                            tableScan(schemaSTableB, sTableB),
                            Join.Type.INNER,
                            "b",
                            eq(cre("col2", sTableB, 4, ResolvedType.of(Type.String)), cre("col2", sTableA, 1, ResolvedType.of(Type.String))),
                            asSet(),
                            false,
                            Schema.EMPTY),
                        new OperatorFunctionScan(
                            Schema.of(col("__expr0", ResolvedType.table(objectArraySchema), null, true)),
                            projection(
                                new ExpressionScan(
                                    e_b.withParent(sTableB),
                                    Schema.of(
                                        col("col1", ResolvedType.of(Type.Boolean), e_b.withParent(sTableB)),
                                        col("col2", ResolvedType.of(Type.String), e_b.withParent(sTableB)),
                                        col("col3", ResolvedType.of(Type.Float), e_b.withParent(sTableB))),
                                    ocre("b", sTableB, 3, ResolvedType.table(schemaSTableB), CoreColumn.Type.POPULATED),
                                    null),
                                asList(
                                    cre("col3", e_b.withParent(sTableB), sTableB, 2, ResolvedType.of(Type.Float)),
                                    cre("col2", e_b.withParent(sTableB), sTableB, 1, ResolvedType.of(Type.String)),
                                    ocre("col3", sTableA, 2, ResolvedType.of(Type.Float))
                                )),
                            "",
                            "object_array",
                            null),
                        Join.Type.LEFT,
                        null,
                        null,
                        Set.of(
                            pop("b", ResolvedType.table(schemaSTableB), sTableB),
                            col("col3", Type.Float, sTableA)
                        ),
                        false,
                        Schema.of(
                            col("col1", Type.Int, sTableA),
                            col("col2", Type.String, sTableA),
                            col("col3", Type.Float, sTableA),
                            pop("b", ResolvedType.table(schemaSTableB), sTableB)
                        )),
                asList(cre("col1", sTableA, 0, ResolvedType.of(Type.Int)),
                       new AliasExpression(new DereferenceExpression(cre("b", sTableB, 3, ResolvedType.table(schemaSTableB), CoreColumn.Type.POPULATED),
                               "col2", 1, ResolvedType.array(ResolvedType.of(Type.String)),
                               new ColumnReference(sTableB, CoreColumn.Type.REGULAR)),
                            "bOuterCol"),
                       ce("__expr0", 4, ResolvedType.table(objectArraySchema))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_populate_join_schema_full()
    {
        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(0, "", "tableB", "b");
        Schema schemaA = Schema.of(col("col", Type.Int, tableA), col("col2", Type.Double, tableA));
        Schema schemaB = Schema.of(col("col", Type.String, tableB), col("col2", Type.Double, tableB));

        //@formatter:off
        ILogicalPlan plan =  projection(
                new Join(
                        tableScan(schemaA, tableA),
                        tableScan(schemaB, tableB),
                        Join.Type.INNER,
                        "b",
                        e("b.col2 = a.col2"),
                        asSet(),
                        false,
                        Schema.EMPTY),
                asList(e("a.col"), e("b.col")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    tableScan(schemaB, tableB),
                    Join.Type.INNER,
                    "b",
                    eq(cre("col2", tableB, 3, ResolvedType.of(Type.Double)), cre("col2", tableA, 1, ResolvedType.of(Type.Double))),
                    asSet(),
                    false,
                    Schema.EMPTY),
                asList(
                        cre("col", tableA, 0, ResolvedType.of(Type.Int)),
                        new DereferenceExpression(cre("b", tableB, 2, ResolvedType.table(schemaB), CoreColumn.Type.POPULATED), "col", 0, ResolvedType.array(ResolvedType.of(Type.String)),
                                new ColumnReference(tableB, CoreColumn.Type.REGULAR)))
                );
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(Schema.of(col("col", Type.Int, tableA), col("col", ResolvedType.array(ResolvedType.of(Type.String)), tableB)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_that_sub_query_expression_with_no_table_source_with_unqualified_column_gets_resolved()
    {
        //@formatter:off
        String query = ""
                + "select col, "
                + "( "
                + "  select col2 "
                + ") \"values\" "
                + "from tableA a ";
        //@formatter:on

        ILogicalPlan plan = getPlanBeforeColumnResolver(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    tableScan(schemaA, tableA),
                    asList(cre("col", tableA), new AliasExpression(cre("col2", tableA), "values"))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_that_sub_query_expression_with_table_source_with_unqualified_column_gets_resolved_to_outer()
    {
        /*
       * @formatter:off
       * 
       * select col1,               <--- tableA
       * (
       *    select b.col1           <--- tableB
       *    ,      col3             <--- tableA outer
       *    from tableB b
       * ) vals
       * from tableA
       * 
       * @formatter:on
       */
        TableSourceReference tableB = of(1, "", "tableB", "b");
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        String query = """
                select col1,                   --- tableA
                (
                  select b.col1                --- tableB
                  ,      col3                  --- stablea outer since it's schema full with col3
                  from tableB b
                  for object
                ) vals
                from stableA a
                """;
        //@formatter:on

        ILogicalPlan actual = getPlanBeforeColumnResolver(query);// optimize(context, plan);
        actual = optimize(context, actual);

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                        tableScan(schemaSTableA, sTableA),
                        new OperatorFunctionScan(
                            Schema.of(col("__expr0", ResolvedType.object(Schema.of(col("col1", Type.Any, tableB), col("col3", Type.Float, sTableA))), null, true)),
                            projection(
                                tableScan(schemaB, tableB),
                                List.of(
                                    cre("col1", tableB),
                                    ocre("col3", sTableA, 2, ResolvedType.of(Type.Float), CoreColumn.Type.REGULAR)
                                )),
                            "",
                            "object",
                            null),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        Set.of(col("col3", Type.Float, sTableA)),
                        false,
                        schemaSTableA),
                    asList(cre("col1", sTableA, 0, ResolvedType.of(Type.Int)),
                            new AliasExpression(ce("__expr0", 3, ResolvedType.object(Schema.of(
                                    col("col1", Type.Any, tableB),
                                    col("col3", Type.Float, sTableA)))), "vals"))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_sub_query_expression()
    {
        //@formatter:off
        String query = ""
                + "select col, "
                + "( "
                + "  select b.col2 "
                + "  from tableB b "
                + ") \"values\" "
                + "from tableA a ";
        //@formatter:on

        ILogicalPlan plan = getPlanBeforeColumnResolver(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(1, "", "tableB", "b");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    new MaxRowCountAssert(
                        projection(
                            tableScan(schemaB, tableB),
                            asList(new AliasExpression(cre("col2", tableB), "__expr0", true))),
                        1),
                    Join.Type.LEFT,
                    null,
                    null,
                    Set.of(),
                    false,
                    Schema.of(ast("a", ResolvedType.ANY, tableA))),
                asList(cre("col", tableA), new AliasExpression(cre("__expr0", tableB), "values"))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_sub_query_expression_outer_references()
    {
        //@formatter:off
        String query = ""
                + "select col, "
                + "( "
                + "  select "
                + "  ( "
                + "    select c.col5 "
                + "    from tableC c "
                + "    where c.col1 > b.col2 "            // b.col2 outer reference
                + "    and c.col2 > a.col3 "              // a.col3 outer outer reference
                + "  ) keys "
                + "  from tableB b "
                + "  where b.col3 > a.col4 "              // a.col4 outer reference
                + ") \"values\" "
                + "from tableA a ";
        //@formatter:on

        ILogicalPlan plan = getPlanBeforeColumnResolver(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(1, "", "tableB", "b");
        TableSourceReference tableC = of(2, "", "tableC", "c");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        Schema schemaC = Schema.of(ast("c", Type.Any, tableC));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    new MaxRowCountAssert(
                        projection(
                            new Join(
                                new Filter(
                                    tableScan(schemaB, tableB),
                                    null,
                                    gt(cre("col3", tableB), ocre("col4", tableA))),
                                new MaxRowCountAssert(
                                    projection(
                                        new Filter(
                                            tableScan(schemaC, tableC),
                                            null,
                                            and(gt(cre("col1", tableC), ocre("col2", tableB)), gt(cre("col2", tableC), ocre("col3", tableA)))),
                                        asList(new AliasExpression(cre("col5", tableC), "__expr1", true))),
                                    1),
                                Join.Type.LEFT,
                                null,
                                null,
                                Set.of(
                                    col("col2", ResolvedType.ANY, tableB)
                                ),
                                false,
                                Schema.of(
                                    ast("a", ResolvedType.ANY, tableA),
                                    ast("b", ResolvedType.ANY, tableB)
                                )),
                            asList(new AliasExpression(cre("__expr1", tableC), "__expr0", true))),
                        1),
                    Join.Type.LEFT,
                    null,
                    null,
                    Set.of(
                        col("col4", ResolvedType.ANY, tableA),
                        col("col3", ResolvedType.ANY, tableA)
                    ),
                    false,
                    Schema.of(
                        ast("a", ResolvedType.ANY, tableA)
                    )),
                asList(cre("col", tableA), new AliasExpression(cre("__expr0", tableC), "values"))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_multiple_aliases_with_same_name_but_on_different_levels()
    {
        /*
         * @formatter:off
         * 
         * 
         * select *
         * from tableA a
         * inner join
         * (
         *   select *
         *   from tableB a              <--- a is fine here since it's another level
         *   where a.col > 10
         * ) x
         *   on x.col2 = a.col2
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "es", "tableA", "a");
        TableSourceReference tableB_a = of(1, "es", "tableB", "a");
        TableSourceReference subQueryX = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB_a = Schema.of(ast("a", Type.Any, tableB_a));

        //@formatter:off
        ILogicalPlan plan = 
                new Join(
                        tableScan(schemaA, tableA),
                        subQuery(
                            new Filter(
                                tableScan(schemaB_a, tableB_a),
                                null,
                                e("a.col > 10")),
                            subQueryX),
                        Join.Type.INNER,
                        null,
                        e("x.col2 = a.col2"),
                        asSet(),
                        false,
                        Schema.EMPTY);
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                        new Join(
                            tableScan(schemaA, tableA),
                            subQuery(
                                new Filter(
                                    tableScan(schemaB_a, tableB_a),
                                    null,
                                    gt(cre("col", tableB_a), intLit(10))),
                                subQueryX),
                            Join.Type.INNER,
                            null,
                            eq(cre("col2", tableB_a), cre("col2", tableA)),
                            asSet(),
                            false,
                            Schema.EMPTY);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_multiple_aliases_with_same_identifier_sub_query()
    {
        /*
         * @formatter:off
         * 
         * 
         * select *
         * from tableA a
         * inner join
         * (
         *   select *
         *   from tableB a
         *   where a.col > 10
         * ) a                          <--- a specified multiple times
         *   on x.col2 = a.col2
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "es", "tableA", "a");
        TableSourceReference tableB = of(1, "es", "tableB", "a");
        TableSourceReference subQueryA = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("a"), "a");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                new Join(
                        tableScan(schemaA, tableA),
                        subQuery(
                            new Filter(
                                tableScan(schemaB, tableB),
                                null,
                                e("a.col = a.col")),
                            subQueryA),
                        Join.Type.INNER,
                        null,
                        e("a.col2 = a.col2"),
                        asSet(),
                        false,
                        Schema.EMPTY);
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of multiple aliases with same name");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Alias 'a' is specified multiple times."));
        }
    }

    @Test
    public void test_multiple_aliases_with_same_identifier_tables()
    {
        /*
         * @formatter:off
         * 
         * 
         * select *
         * from tableA a
         * inner join tableB a
         *   on a.col2 = a.col2
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "es", "tableA", "a");
        TableSourceReference tableB = of(1, "es", "tableB", "a");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                new Join(
                        tableScan(schemaA, tableA),
                        tableScan(schemaB, tableB),
                        Join.Type.INNER,
                        null,
                        e("a.col2 = a.col2"),
                        asSet(),
                        false,
                        Schema.EMPTY);
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of multiple aliases with same name");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Alias 'a' is specified multiple times."));
        }
    }

    @Test
    public void test_multiple_aliases_with_same_identifier_functions()
    {
        /*
         * @formatter:off
         * 
         * 
         * select *
         * from tableA a
         * outer apply func() a
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "es", "tableA", "a");
        TableSourceReference range = new TableSourceReference(1, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "a");

        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, range));

        //@formatter:off
        ILogicalPlan plan = 
                new Join(
                        tableScan(schemaA, tableA),
                        new TableFunctionScan(range, schemaB, emptyList(), emptyList(), null),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        false,
                        Schema.EMPTY);
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of multiple aliases with same name");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Alias 'a' is specified multiple times."));
        }
    }

    @Test
    public void test_crorrlated_joins_no_bindable()
    {
        String query = "" + "select * "
                       + "from tableA a "
                       + "inner join "
                       + "( "
                       + "  select * "
                       + "  from tableB b "
                       + "  where b.col = a.col " // a.col references outer and is not allowed in joins
                       + ") x"
                       + "  on x.col2 = a.col2 ";

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        try
        {
            optimize(context, plan);
            fail("Should fail cause a.col not found");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("a.col cannot be bound"));
        }
    }

    @Test
    public void test_subquery_with_cross_join_projecting_multi_table_source_schema_less()
    {
        /*
         * @formatter:off
         * 
         * select x.col
         * from
         * (
         *   select a.col
         *   from tableA a
         *   outer apply tableB b
         * ) x
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "es", "tableA", "a");
        TableSourceReference tableB = of(1, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        projection(
                            new Join(
                                tableScan(schemaA, tableA),
                                tableScan(schemaB, tableB),
                                Join.Type.LEFT,
                                null,
                                (IExpression) null,
                                asSet(),
                                false,
                                Schema.EMPTY),
                            asList(e("a.col"))),
                        subQueryX),
                    asList(e("x.col")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                    projection(
                        subQuery(
                            projection(
                                new Join(
                                    tableScan(schemaA, tableA),
                                    tableScan(schemaB, tableB),
                                    Join.Type.LEFT,
                                    null,
                                    (IExpression) null,
                                    asSet(),
                                    false,
                                    schemaA),
                                asList(cre("col", tableA)),
                                subQueryX),
                            subQueryX),
                        asList(cre("col", tableA, 0)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_subquery_with_nested_projections_computed_values()
    {
        /*
         * @formatter:off
         * 
         * select x.col
         * from
         * (
         *   select a.col1 + a.col2 col,
         *   a.col3                         <-- not projected in outer projection and should not be present in result
         *   from tableA a
         * ) x
         *
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "es", "tableA", "a");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        projection(
                            tableScan(schemaA, tableA),
                            asList(new AliasExpression(e("a.col1 + a.col2"), "col"), e("a.col3"))),
                        subQueryX),
                    asList(e("x.col")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                    projection(
                        subQuery(
                            projection(
                                tableScan(schemaA, tableA),
                                asList(new AliasExpression(add(cre("col1", tableA), cre("col2", tableA)), "col"), cre("col3", tableA)),
                                subQueryX),
                            subQueryX),
                    asList(cre("col", subQueryX, 0)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_correlated_subquery_schema_less()
    {
        /*
         * @formatter:off
         * 
         * 
         * select *
         * from tableA a
         * cross apply
         * (
         *   select *
         *   from tableB b
         *   where b.col = a.col      <-- Reference to outer schema
         * ) x
         * where x.col2 = a.col2
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "es", "tableA", "a");
        TableSourceReference tableB = of(1, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                new Filter(
                    new Join(
                            tableScan(schemaA, tableA),
                            subQuery(
                                new Filter(
                                    tableScan(schemaB, tableB),
                                    null,
                                    e("b.col = a.col")),
                                subQueryX),
                            Join.Type.INNER,
                            null,
                            (IExpression) null,
                            asSet(),
                            false,
                            Schema.EMPTY),
                    null,
                    e("x.col2 = a.col2"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        tableScan(schemaA, tableA),
                        subQuery(
                            new Filter(
                                tableScan(schemaB, tableB),
                                null,
                                eq(cre("col", tableB), ocre("col", tableA))),
                            subQueryX),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(col("col", Type.Any, tableA)),
                        false,
                        schemaA),
                    null,
                    eq(cre("col2", tableB), cre("col2", tableA)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_qualified_asterisk_select_non_existent_alias()
    {
        //@formatter:off
        String query = "" 
                + "select c.* "
                + "from tableA a "
                + "inner join "
                + "( "
                + "  select * "
                + "  from tableB b "
                + ") x"
                + "  on x.col2 = a.col2 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        try
        {
            optimize(context, plan);
            fail("Should fail alias c cannot be bound");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Alias c could not be bound"));
        }
    }

    @Test
    public void test_correlated_subquery_projecting_outer_schema_less()
    {
        /*
         * @formatter:off
         * 
         * 
         * select *
         * from tableA a
         * cross apply
         * (
         *   select b.col2, a.col3    <-- Projecting outer schema
         *   from tableB b
         *   where b.col = a.col      <-- Reference to outer schema
         * ) x
         * where x.col2 = a.col2
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(1, "", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                new Filter(
                    new Join(
                            tableScan(schemaA, tableA),
                            subQuery(
                                projection(
                                    new Filter(
                                        tableScan(schemaB, tableB),
                                        null,
                                        e("b.col = a.col")),
                                    asList(e("b.col2"), e("a.col3"))),
                                subQueryX),
                            Join.Type.INNER,
                            null,
                            (IExpression) null,
                            asSet(),
                            false,
                            Schema.EMPTY),
                    null,
                    e("x.col2 = a.col2"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        tableScan(schemaA, tableA),
                        subQuery(
                            projection(
                                new Filter(
                                    tableScan(schemaB, tableB),
                                    null,
                                    eq(cre("col", tableB), ocre("col", tableA))),
                                asList(cre("col2", tableB), ocre("col3", tableA)),
                                subQueryX),
                            subQueryX),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(col("col", Type.Any, tableA), col("col3", Type.Any, tableA)),
                        false,
                        schemaA),
                    null,
                    eq(cre("col2", tableB, CoreColumn.Type.REGULAR), cre("col2", tableA)));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(ast("a", Type.Any, tableA), col("col2", Type.Any, tableB), col("col3", Type.Any, tableA)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_correlated_subquery_projecting_outer_schema_full()
    {
        /*
         * @formatter:off
         * 
         * 
         * select *
         * from tableA a
         * cross apply
         * (
         *   select b.col2, a.col3    <-- Projecting outer schema
         *   from tableB b
         *   where b.col = a.col      <-- Reference to outer schema
         * ) x
         * where x.col2 = a.col2
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(1, "", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaA = Schema.of(col("col", Type.String, tableA), col("col3", Type.Int, tableA), col("col2", Type.Any, tableA));
        Schema schemaB = Schema.of(col("col", Type.Int, tableB), col("col2", Type.Float, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                new Filter(
                    new Join(
                            tableScan(schemaA, tableA),
                            subQuery(
                                projection(
                                    new Filter(
                                        tableScan(schemaB, tableB),
                                        null,
                                        e("b.col = a.col")),
                                    asList(e("b.col2"), e("a.col3"))),
                                subQueryX),
                            Join.Type.INNER,
                            null,
                            (IExpression) null,
                            asSet(),
                            false,
                            Schema.EMPTY),
                    null,
                    e("x.col2 = a.col2"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        tableScan(schemaA, tableA),
                        subQuery(
                            projection(
                                new Filter(
                                    tableScan(schemaB, tableB),
                                    null,
                                    eq(cre("col", tableB, 0, ResolvedType.of(Type.Int)), ocre("col", tableA, 0, ResolvedType.of(Type.String)))),
                                asList(cre("col2", tableB, 1, ResolvedType.of(Type.Float)), ocre("col3", tableA, 1, ResolvedType.of(Type.Int))),
                                subQueryX),
                            subQueryX),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(col("col", Type.String, tableA), col("col3", Type.Int, tableA)),
                        false,
                        schemaA),
                    null,
                    eq(cre("col2", tableB, 3, ResolvedType.of(Type.Float)), cre("col2", tableA, 2, ResolvedType.of(Type.Any))));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .isEqualTo(
                //@formatter:off
                    Schema.of(
                        // 1:st all table a columns
                        col("col", Type.String, tableA),
                        col("col3", Type.Int, tableA),
                        col("col2", Type.Any, tableA),

                        // ... then all sub query columns
                        col("col2", Type.Float, tableB),
                        col("col3", Type.Int, tableA))
                    //@formatter:on
                );

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_correlated_subquery_schema_less()
    {
        String query = "" + "select * "
                       + "from tableA a "
                       + "cross apply "
                       + "( "
                       + "  select b.col2, * "
                       + "  from tableB b "
                       + "  cross apply "
                       + "  ( "
                       + "    select c.col3, * "
                       + "    from tableC c "
                       + "    where c.col = b.col " // b.col = Reference to outer schema
                       + "    and c.col2 = a.col2 " // a.col2 = Reference to outer outer schema
                       + "  ) y "
                       + "  where b.col = a.col " // a.col = Reference to outer schema
                       + "  and y.col3 = b.col3 "
                       + ") x "
                       + "where x.col2 = a.col2 ";

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(2, "", "tableB", "b");
        TableSourceReference tableC = of(4, "", "tableC", "c");
        TableSourceReference subQueryY = new TableSourceReference(3, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("y"), "y");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        Schema schemaC = Schema.of(ast("c", Type.Any, tableC));
        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Filter(
                        new Join(
                            tableScan(schemaA, tableA),
                            subQuery(
                                projection(
                                    new Filter(
                                        new Join(
                                            tableScan(schemaB, tableB),
                                            subQuery(
                                                projection(
                                                    new Filter(
                                                        tableScan(schemaC, tableC),
                                                        null,
                                                        and(eq(cre("col", tableC), ocre("col", tableB)), eq(cre("col2", tableC), ocre("col2", tableA)))),
                                                    asList(cre("col3", tableC), new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(tableC))),
                                                    subQueryY),
                                                subQueryY),
                                            Join.Type.INNER,
                                            null,
                                            (IExpression) null,
                                            asSet(
                                                col("col", Type.Any, tableB) // Only direct outer reference
                                            ),
                                            false,
                                            SchemaUtils.joinSchema(schemaA, schemaB)
                                        ),
                                        null,
                                        and(eq(cre("col", tableB), ocre("col", tableA)), eq(cre("col3", tableC, CoreColumn.Type.REGULAR), cre("col3", tableB)))),
                                    asList(cre("col2", tableB), new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(tableB, tableC))),
                                    subQueryX),
                                subQueryX),
                            Join.Type.INNER,
                            null,
                            (IExpression) null,
                            asSet(
                                col("col2", Type.Any, tableA),
                                col("col", Type.Any, tableA)),
                            false,
                            schemaA),
                        null,
                        eq(cre("col2", tableB, CoreColumn.Type.REGULAR), cre("col2", tableA))),
                    List.of(new AsteriskExpression(QualifiedName.of(), null, Set.of(tableA, tableB, subQueryX))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                ast("a", Type.Any, tableA),
                col("col2", Type.Any, tableB),
                new CoreColumn("", ResolvedType.of(Type.Any), "*", false, subQueryX, CoreColumn.Type.ASTERISK)));
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_correlated_subquery_schema_less_2()
    {
        String query = "select * " + "from tableA a "
                       + "cross apply " // ------------- This join has an outer reference in the inner most join a.col2
                       + "( "
                       + "  select b.col2, * "
                       + "  from tableB b "
                       + "  cross apply " // ----------- This join doesn't have any direct outer references
                       + "  ( " // --------------------- and can be a non looping variant
                       + "    select c.col3, * "
                       + "    from tableC c "
                       + "    where c.col2 = a.col2 " // -- a.col2 = Reference to outer outer schema
                       + "  ) y "
                       + "  where y.col3 = b.col3 "
                       + ") x "
                       + "where x.col2 = a.col2 ";

        ILogicalPlan plan = getPlanBeforeColumnResolver(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of(0, "", "tableA", "a");
        TableSourceReference tableB = of(2, "", "tableB", "b");
        TableSourceReference tableC = of(4, "", "tableC", "c");
        TableSourceReference subQueryY = new TableSourceReference(3, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("y"), "y");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        Schema schemaC = Schema.of(ast("c", Type.Any, tableC));
        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Filter(
                        new Join(
                            tableScan(schemaA, tableA),
                            subQuery(
                                projection(
                                    new Filter(
                                        new Join(
                                            tableScan(schemaB, tableB),
                                            subQuery(
                                                projection(
                                                    new Filter(
                                                        tableScan(schemaC, tableC),
                                                        null,
                                                        eq(cre("col2", tableC), ocre("col2", tableA))),
                                                    asList(cre("col3", tableC), new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(tableC))),
                                                    subQueryY),
                                                subQueryY),
                                            Join.Type.INNER,
                                            null,
                                            (IExpression) null,
                                            asSet(),
                                            false,
                                            SchemaUtils.joinSchema(schemaA, schemaB)
                                        ),
                                        null,
                                        eq(cre("col3", tableC, CoreColumn.Type.REGULAR), cre("col3", tableB))),
                                    asList(cre("col2", tableB), new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(tableB, tableC))),
                                    subQueryX),
                                subQueryX),
                            Join.Type.INNER,
                            null,
                            (IExpression) null,
                            asSet(
                                col("col2", Type.Any, tableA)
                            ),
                            false,
                            schemaA),
                        null,
                        eq(cre("col2", tableB, CoreColumn.Type.REGULAR), cre("col2", tableA))),
                    List.of(new AsteriskExpression(QualifiedName.EMPTY, null, Set.of(tableA, tableB, subQueryX))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                ast("a", Type.Any, tableA),
                col("col2", Type.Any, tableB),
                new CoreColumn("", ResolvedType.of(Type.Any), "*", false, subQueryX, CoreColumn.Type.ASTERISK)));
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_subquery_with_join_projecting_multi_table_source_schema_less()
    {
        /*
         * @formatter:off
         * 
         * Here x maps to two table sources
         * (tableB and tableA)
         * 
         * select x.col
         * from
         * (
         *   select *
         *   from tableB b
         *   inner join tableA a
         *    on ...
         * ) x
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "es", "tableA", "a");
        TableSourceReference tableB = of(1, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        new Join(
                            tableScan(schemaA, tableA),
                            tableScan(schemaB, tableB),
                            Join.Type.INNER,
                            null,
                            e("b.col = a.col"),
                            asSet(),
                            false,
                            Schema.EMPTY),
                        subQueryX),
                    asList(e("x.col")));
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of ambiguous column");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Ambiguous column: x.col"));
        }
    }

    @Test
    public void test_subquery_with_join_projecting_multi_table_source_with_schema()
    {
        /*
         * @formatter:off
         * 
         * Here x maps to two table sources
         * (tableB and tableA)
         * 
         * select x.col1
         * from
         * (
         *   select *
         *   from tableB b
         *   inner join tableA a
         *    on ...
         * ) x
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "es", "tableA", "a");
        TableSourceReference tableB = of(1, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");

        Schema schemaA = Schema.of(col("col", Type.Any, tableA), col("col1", Type.Any, tableA));
        Schema schemaB = Schema.of(col("col", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        new Join(
                            tableScan(schemaA, tableA),
                            tableScan(schemaB, tableB),
                            Join.Type.INNER,
                            null,
                            e("b.col = a.col"),
                            asSet(),
                            false,
                            Schema.EMPTY),
                        subQueryX),
                    asList(e("x.col1")));
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of multiple column names");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("The column 'col' was specified multiple times for 'x'"));
        }
    }

    @Test
    public void test_subquery_with_join_projecting_multi_table_source_with_schema_ambiguity()
    {
        /*
         * @formatter:off
         * 
         * Here x maps to two table sources
         * (tableB and tableA)
         * 
         * select x.col             <--- col is ambiguous since it's located on both tables
         * from
         * (
         *   select *
         *   from tableB b
         *   inner join tableA a
         *    on ...
         * ) x
         * 
         * @formatter:on
         */

        TableSourceReference tableA = of(0, "es", "tableA", "a");
        TableSourceReference tableB = of(1, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");

        Schema schemaA = Schema.of(ast("col", Type.Any, tableA), col("col1", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("col", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        new Join(
                            tableScan(schemaA, tableA),
                            tableScan(schemaB, tableB),
                            Join.Type.INNER,
                            null,
                            e("b.col = a.col"),
                            asSet(),
                            false,
                            Schema.EMPTY),
                        subQueryX),
                    asList(e("x.col")));
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of ambiguous column");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Ambiguous column: x.col"));
        }
    }

    @Test
    public void test_table_with_joined_subquerys_schema_less()
    {
        /*
         * @formatter:off
         * 
         * select *
         * from tableA a
         * inner join 
         * (
         *   select *
         *   from tableB b
         * ) x
         *   on x.col = a.col
         * 
         * join: x.col = a.col
         *   scan: tableA
         *   subQuery: x
         *     scan: tableB

         * join: x.col = a.col
         *   scan: tableA a
         *   scan: tableB b
         * 
         * @formatter:on
         * 
         * QueryParser
         *  visit TableScan (alias a)
         *  Ask catalog for schema, if static recreate all columns with correct alias
         *  
         * 
         */

        TableSourceReference tableA = of(0, "es", "tableA", "a");
        TableSourceReference tableB = of(1, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                new Join(
                    tableScan(schemaA, tableA),
                    subQuery(
                        tableScan(schemaB, tableB),
                        subQueryX),
                    Join.Type.INNER,
                    null,
                    e("x.col = a.col"),
                    asSet(),
                    false,
                    Schema.EMPTY);
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Join(
                        tableScan(schemaA, tableA),
                        subQuery(
                            tableScan(schemaB, tableB),
                            subQueryX),
                    Join.Type.INNER,
                    null,
                    eq(cre("col", tableB), cre("col", tableA)),
                    asSet(),
                    false,
                    Schema.EMPTY);
        //@formatter:on
        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_subqueries_with_filters_schema_less()
    {
        /*
         * @formatter:off
         * 
         * select *
         * from
         * (
         *   select *
         *   from tableB b
         *   where b.col1 > 10
         * ) x
         * where x.col > 10
         * 
         * filter: x.col > 10
         *   subQuery: x
         *     filter b.col1 > 10
         *       scan: tableB
         * 
         * @formatter:on
         * 
         */

        TableSourceReference tableB = of(0, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                new Filter(
                    subQuery(
                        new Filter(
                            tableScan(schemaB, tableB),
                            null,
                            e("b.col1 > 10")),
                        subQueryX),
                    null,
                    e("x.col > 20"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    subQuery(
                        new Filter(
                            tableScan(schemaB, tableB),
                            null,
                            gt(cre("col1", tableB), new LiteralIntegerExpression(10))),
                        subQueryX),
                    null,
                    gt(cre("col", tableB), new LiteralIntegerExpression(20)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(schemaB, actual.getSchema());

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_subqueries_with_filters_with_schema()
    {
        /*
         * @formatter:off
         * 
         * select *
         * from
         * (
         *   select *
         *   from tableB b
         *   where b.col1 > 10
         * ) x
         * where x.col > 10
         * 
         * filter: x.col > 10
         *   subQuery: x
         *     filter b.col1 > 10
         *       scan: tableB
         * 
         * @formatter:on
         * 
         */

        TableSourceReference tableB = of(0, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Column col1 = col("col1", Type.Float, tableB);
        Column col = col("col", Type.Double, tableB);

        Schema schemaB = Schema.of(col1, col);

        //@formatter:off
        ILogicalPlan plan = 
                new Filter(
                    subQuery(
                        new Filter(
                            tableScan(schemaB, tableB),
                            null,
                            e("b.col1 > 10")),
                    subQueryX),
                    null,
                    e("x.col > 20"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    subQuery(
                        new Filter(
                            tableScan(schemaB, tableB),
                            null,
                            gt(cre("col1", tableB, 0, ResolvedType.of(Type.Float)),
                               intLit(10))),
                        subQueryX),
                    null,
                        gt(cre("col", tableB, 1, ResolvedType.of(Type.Double)),
                            intLit(20)) );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(schemaB, expected.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_unnamed_sub_query_columns()
    {
        //@formatter:off
        String query = ""
                + "select * "
                + "from "
                + "( "
                + "  select a.col1 + a.col1 "
                + "  from \"table\" a "
                + ") x";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        try
        {
            optimize(context, plan);
            fail("Should fail cause of missing column name");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Missing column name for ordinal 0 of x"));
        }
    }

    @Test
    public void test_nested_subqueries_with_projections_schema_less()
    {
        /*
         * @formatter:off
         * 
         * select x.col1, x.col2, x.col3 + x.col2
         * from
         * (
         *   select col1, col2, col3
         *   from tableB b
         * ) x
         * 
         * @formatter:on
         */

        TableSourceReference tableB = of(0, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        projection(
                            tableScan(schemaB, tableB),
                            asList(e("col1"), e("col2"), e("col3"))),
                        subQueryX),
                    asList(e("x.col1"), e("x.col2"), e("x.col3 + x.col2")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    subQuery(
                        projection(
                            tableScan(schemaB, tableB),
                            asList(cre("col1", tableB), cre("col2", tableB), cre("col3", tableB)),
                            subQueryX),
                        subQueryX),
                    asList(cre("col1", tableB, 0), cre("col2", tableB, 1), add(cre("col3", tableB, 2), cre("col2", tableB, 1))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_subqueries_with_projections_with_schema()
    {
        /*
         * @formatter:off
         * 
         * select x.col1,x.col2, x.col3 + x.col2
         * from
         * (
         *   select col1, col2, col3
         *   from tableB b
         * ) x
         * 
         * projection: x.col1,x.col2, x.col3 + x.col2
         *   subQuery
         *     projection col1, col2, col3
         *       scan: tableB

         * projection: col1[0], col2[1], col3[2] + col2[1]
         *     projection col1, col2, col3
         *       scan: tableB

         * projection: col1[0], col2[1], col3[2] + col2[1]
         *     scan: tableB
         * 
         * @formatter:on
         */

        TableSourceReference tableB = of(0, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        CoreColumn col1 = col("col1", Type.Int, tableB);
        CoreColumn col2 = col("col2", Type.Float, tableB);
        CoreColumn col3 = col("col3", Type.String, tableB);
        Schema schemaB = Schema.of(col1, col2, col3);

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        projection(
                            tableScan(schemaB, tableB),
                            asList(e("col1"), e("col2"), e("col3"))),
                        subQueryX),
                    asList(e("x.col1"), e("x.col2"), e("x.col3 + x.col2")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                    projection(
                        subQuery(
                            projection(
                                tableScan(schemaB, tableB),
                                List.of(cre("col1", tableB, 0, ResolvedType.of(Type.Int)),
                                        cre("col2", tableB, 1, ResolvedType.of(Type.Float)),
                                        cre("col3", tableB, 2, ResolvedType.of(Type.String))),
                                subQueryX),
                            subQueryX),
                        List.of(cre("col1", tableB, 0, ResolvedType.of(Type.Int)),
                                cre("col2", tableB, 1, ResolvedType.of(Type.Float)),
                                add(
                                        cre("col3", tableB, 2, ResolvedType.of(Type.String)),
                                        cre("col2", tableB, 1, ResolvedType.of(Type.Float)))));
        //@formatter:on

        Schema expectedSchema = Schema.of(col1, col2, new CoreColumn("", ResolvedType.of(Type.Float), "b.col3 + b.col2", false));
        assertEquals(expectedSchema, actual.getSchema());

        // System.out.println(expectedSchema);
        // System.out.println(actual.getSchema());
        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_subqueries_are_eliminated()
    {
        TableSourceReference table = of(0, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        TableSourceReference subQueryY = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("y"), "y");
        TableSourceReference subQueryZ = new TableSourceReference(3, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("z"), "z");
        Schema schema = Schema.of(ast("b", Type.Any, table));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        subQuery(
                            subQuery(
                                tableScan(schema, table),
                            subQueryX), 
                        subQueryY),
                    subQueryZ),
                    asList(e("z.col1"), e("col2")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    subQuery(
                        subQuery(
                            subQuery(
                                tableScan(schema, table),
                                subQueryX),
                            subQueryY),
                        subQueryZ),
                    asList(cre("col1", table), cre("col2", table)));
        //@formatter:on

        Schema expectedSchema = Schema.of(col("col1", Type.Any, table), col("col2", Type.Any, table));

        // System.out.println(actual.getSchema());
        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expectedSchema);

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_projection_with_single_sub_query_schema_less()
    {
        // select x.col1, col2
        // * from
        // * (
        // * select *
        // * from tableB b
        // * ) x

        TableSourceReference table = of(0, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");

        Schema schema = Schema.of(ast("b", Type.Any, table));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        tableScan(schema, table),
                        subQueryX),
                    asList(e("x.col1"), e("col2")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    subQuery(
                        tableScan(schema, table),
                        subQueryX),
                    asList(cre("col1", table), cre("col2", table)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    public void test_projection_with_join_static_schema_ambiguity()
    {
        /*
         * Static schema And col2 is present on both tables => throw queryexception
         * 
         * select b.col1, col2 <--- col2 exists on both tables from tableB inner join tableC c on c.col1 = b.col1
         */

        TableSourceReference tableB = of(0, "es", "tableB", "b");
        TableSourceReference tableC = of(1, "es", "tableC", "c");
        Schema schemaB = Schema.of(col("col1", Type.Int, tableB), col("col2", Type.String, tableB));
        Schema schemaC = Schema.of(col("col1", Type.Int, tableC), col("col2", Type.Int, tableC), col("col3", Type.String, tableC));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    new Join(
                        tableScan(schemaB, tableB),
                        tableScan(schemaC, tableC),
                        Join.Type.INNER,
                        null,
                        e("c.col1 = b.col1"),
                        asSet(),
                        false,
                        Schema.EMPTY),
                    asList(e("b.col1"), e("col2")));
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of ambiguous column");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Ambiguous column: col2"));
        }
    }

    @Test
    public void test_projection_with_join_non_static_schema_ambiguity()
    {
        /* @formatter:off
         * select b.col1, col2 <--- col2 is unknown due to ambiguity in a schema less query
         * from tableB 
         * inner join tableC c 
         *   on c.col1 = b.col1
         * @formatter:on
         */

        TableSourceReference tableB = of(0, "es", "tableB", "b");
        TableSourceReference tableC = of(1, "es", "tableC", "c");
        Schema schemaB = Schema.of(ast("b", Type.Any, tableB));
        Schema schemaC = Schema.of(ast("c", Type.Any, tableC));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    new Join(
                        tableScan(schemaB, tableB),
                        tableScan(schemaC, tableC),
                        Join.Type.INNER,
                        null,
                        e("c.col1 = b.col1"),
                        asSet(),
                        false,
                        Schema.EMPTY),
                    asList(e("b.col1"), e("col2")));
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of ambiguous column");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Ambiguous column: col2"));
        }
    }

    @Test
    public void test_projection_with_sub_query_and_join_static_schema()
    {
        /*
         * Static schema And col2 is present on both tables => throw queryexception
         * @formatter:off
         * select x.col1, x.col2 
         * from 
         * ( 
         *   select * 
         *   from tableB b 
         *   inner join tableC c
         *     on c.col1 = b.col1
         * ) x
         * @formatter:on
         */

        TableSourceReference tableB = of(0, "es", "tableB", "b");
        TableSourceReference tableC = of(1, "es", "tableC", "c");
        TableSourceReference subQueryX = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaB = Schema.of(col("col1", Type.Int, tableB), col("col2", Type.String, tableB));
        Schema schemaC = Schema.of(col("col1", Type.Int, tableC), col("col2", Type.Int, tableC), col("col3", Type.String, tableC));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        new Join(
                            tableScan(schemaB, tableB),
                            tableScan(schemaC, tableC),
                            Join.Type.INNER,
                            null,
                            e("c.col1 = b.col1"),
                            asSet(),
                            false,
                            Schema.EMPTY),
                        subQueryX),
                    asList(e("x.col1"), e("x.col2")));
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of multiple columns");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("The column 'col1' was specified multiple times for 'x'"));
        }
    }

    @Test
    public void test_projection_with_static_schema_missing_column()
    {
        /*
         * Static schema And col2 is present on both tables => throw queryexception
         * 
         * select x.col1, x.col2 from ( select * from tableB b inner join tableC c on c.col1 = b.col1 ) x
         */

        TableSourceReference tableB = of(0, "es", "tableB", "b");
        TableSourceReference tableC = of(1, "es", "tableC", "c");
        TableSourceReference subQueryX = new TableSourceReference(2, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schemaB = Schema.of(col("col1", Type.Int, tableB), col("col2", Type.String, tableB));
        Schema schemaC = Schema.of(col("col1", Type.Int, tableC), col("col2", Type.Int, tableC), col("col3", Type.String, tableC));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        new Join(
                            tableScan(schemaB, tableB),
                            tableScan(schemaC, tableC),
                            Join.Type.INNER,
                            null,
                            e("c.col1 = b.col1"),
                            asSet(),
                            false,
                            Schema.EMPTY),
                        subQueryX),
                    asList(e("x.col5"), e("x.col2")));
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of multiple columns");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("The column 'col1' was specified multiple times for 'x'"));
        }
    }

    @Test
    public void test_projection_with_sub_query_static_schema()
    {
        // select x.col1, col2
        // * from
        // * (
        // * select *
        // * from tableB b
        // * ) x

        TableSourceReference tableB = of(0, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schema = Schema.of(col("col1", Type.Int, tableB), col("col2", Type.String, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        tableScan(schema, tableB),
                        subQueryX),
                    asList(e("x.col1"), e("col2")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    subQuery(
                        tableScan(schema, tableB),
                        subQueryX),
                    asList(cre("col1", tableB, 0, ResolvedType.of(Type.Int)), cre("col2", tableB, 1, ResolvedType.of(Type.String))));
        //@formatter:on

        // System.out.println(expected.getSchema());
        // System.out.println(actual.getSchema());
        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(schema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_filter_with_sub_query_static_schema()
    {
        TableSourceReference tableB = of(0, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schema = Schema.of(col("col1", Type.Int, tableB), col("col2", Type.String, tableB));

        //@formatter:off
        ILogicalPlan plan = 
                new Filter(
                    subQuery(
                        tableScan(schema, tableB),
                        subQueryX),
                    null,
                    e("x.col1 > 10"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    subQuery(
                        tableScan(schema, tableB),
                        subQueryX),
                    null,
                    gt(cre("col1", tableB, 0, ResolvedType.of(Type.Int)), new LiteralIntegerExpression(10)));
                    
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(schema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_filter_with_sub_query_schema_less()
    {
        TableSourceReference table = of(0, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schema = Schema.of(ast("b", Type.Any, table));

        //@formatter:off
        ILogicalPlan plan = 
                new Filter(
                    subQuery(
                        tableScan(schema, table),
                        subQueryX),
                    null,
                    e("x.col1 > 10"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    subQuery(
                        tableScan(schema, table),
                        subQueryX),
                    null,
                    gt(cre("col1", table), new LiteralIntegerExpression(10)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(schema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_table_function_resolving()
    {
        ILogicalPlan plan = s("select * from range(1, 10) x");
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableSource = new TableSourceReference(0, TableSourceReference.Type.FUNCTION, "", QualifiedName.of("range"), "x");

        Schema expectedSchema = Schema.of(new CoreColumn("Value", ResolvedType.of(Type.Int), "", false, tableSource, CoreColumn.Type.REGULAR));

        //@formatter:off
        ILogicalPlan expected =
                    projection(
                        new TableFunctionScan(
                                tableSource,
                                expectedSchema,
                                asList(intLit(1), intLit(10)),
                                emptyList(),
                                null),
                        List.of(cre("Value", tableSource, 0, ResolvedType.INT)));
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expectedSchema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_sort_with_sub_query_static_schema()
    {
        TableSourceReference table = of(0, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schema = Schema.of(col("col1", Type.Int, table), col("col2", Type.String, table));

        //@formatter:off
        ILogicalPlan plan = 
                new Sort(
                    subQuery(
                        tableScan(schema, table),
                        subQueryX),
                    asList(new SortItem(e("x.col1"), Order.ASC, NullOrder.FIRST, null)));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    subQuery(
                        tableScan(schema, table),
                        subQueryX),
                    asList(new SortItem(cre("col1", table, 0, ResolvedType.of(Type.Int)), Order.ASC, NullOrder.FIRST, null)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(schema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_sort_with_sub_query_non_static_schema()
    {
        TableSourceReference table = of(0, "es", "tableB", "b");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schema = Schema.of(ast("b", Type.Any, table));

        //@formatter:off
        ILogicalPlan plan = 
                new Sort(
                    subQuery(
                        tableScan(schema, table),
                        subQueryX),
                    asList(new SortItem(e("x.col1"), Order.ASC, NullOrder.FIRST, null)));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    subQuery(
                        tableScan(schema, table),
                        subQueryX),
                    asList(new SortItem(cre("col1", table), Order.ASC, NullOrder.FIRST, null)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(schema, actual.getSchema());
        assertEquals(expected, actual);
    }

    private ILogicalPlan optimize(IExecutionContext context, ILogicalPlan plan)
    {
        ALogicalPlanOptimizer.Context ctx = optimizer.createContext(context);
        return optimizer.optimize(ctx, plan);
    }

    public static TableSourceReference of(int id, String catalogAlias, String name, String alias)
    {
        return new TableSourceReference(id, TableSourceReference.Type.TABLE, catalogAlias, QualifiedName.of(name), alias);
    }
}
