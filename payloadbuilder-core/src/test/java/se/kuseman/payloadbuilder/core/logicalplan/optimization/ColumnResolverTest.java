package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.Random;

import org.antlr.v4.runtime.CommonToken;
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
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedSubQueryExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.parser.ParseException;

/** Test of {@link ColumnResolver} */
// CSOFF
public class ColumnResolverTest extends ALogicalPlanOptimizerTest
// CSON
{
    private final ColumnResolver optimizer = new ColumnResolver();

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

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference a_b = of("", "a.b", "x");

        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference a_bAst = new ColumnReference(a_b, "x", ColumnReference.Type.ASTERISK);

        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaA_b = Schema.of(col("x", Type.Any, a_bAst));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                        tableScan(schemaA, tableA),
                        new ExpressionScan(a_b, schemaA_b, ocre(aAst.rename("b")), null),
                        Join.Type.INNER,
                        null,
                        null,
                        asSet(col(aAst.rename("b"), Type.Any)),
                        false),
                    asList(cre(aAst), cre(a_bAst)));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(Schema.of(new CoreColumn("a", ResolvedType.of(Type.Any), aAst), new CoreColumn("x", ResolvedType.of(Type.Any), a_bAst)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference e_col3 = of("", "e.col3", "x");

        ColumnReference sTableE_col3 = new ColumnReference(sTableE, "col3", ColumnReference.Type.REGULAR);

        ColumnReference e_col3_nCol1 = new ColumnReference(e_col3, "nCol1", ColumnReference.Type.REGULAR);
        ColumnReference e_col3_nCol2 = new ColumnReference(e_col3, "nCol2", ColumnReference.Type.REGULAR);

        Schema schemae_sTableE = Schema.of(col("nCol1", Type.Int), col("nCol2", Type.String));
        Schema schemae_col3 = Schema.of(col("nCol1", Type.Int, e_col3_nCol1), col("nCol2", Type.String, e_col3_nCol2));

        //@formatter:off
        ILogicalPlan expected =
                new Join(
                    tableScan(schemaSTableE, sTableE),
                    new ExpressionScan(e_col3, schemae_col3, ocre("col3", sTableE_col3, 1, ResolvedType.table(schemaSTableE.getColumns().get(1).getType().getSchema())), null),
                    Join.Type.INNER,
                    null,
                    null,
                    asSet(CoreColumn.of(sTableE_col3, ResolvedType.table(schemaSTableE.getColumns().get(1).getType().getSchema()))),
                    false);
        //@formatter:on

        //@formatter:off
        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(Schema.of(
                        col(sTableE.column("col1"), Type.Double),
                        CoreColumn.of(sTableE.column("col3"), ResolvedType.table(schemae_sTableE)),
                        col(sTableE.column("col6"), Type.String),
                        col(e_col3.column("nCol1"), Type.Int),
                        col(e_col3.column("nCol2"), Type.String)
                        ));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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

        TableSourceReference tableA = of("", "tableA", "a");

        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);

        Schema schemaA = Schema.of(col("a", Type.Any, aAst));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    tableScan(schemaA, tableA),
                    asList(DereferenceExpression.create(cre(aAst.rename("multi")), QualifiedName.of(asList("part", "qualifier")), null)));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(Schema.of(new CoreColumn("qualifier", ResolvedType.of(Type.Any), aAst.rename("multi"))));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of("", "tableA", "a");

        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);

        Schema schemaA = Schema.of(col("a", Type.Any, aAst));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    tableScan(schemaA, tableA),
                    asList(new AliasExpression(
                        new UnresolvedSubQueryExpression(
                            projection(
                                ConstantScan.INSTANCE,
                                asList(DereferenceExpression.create(ocre(aAst.rename("multi")), QualifiedName.of(asList("part", "qualifier")), null))
                            ),
                            asSet(CoreColumn.of(aAst.rename("multi"), ResolvedType.of(Type.Any))),
                            null), "val")));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(Schema.of(CoreColumn.of("val", ResolvedType.of(Type.Any))));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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

        TableSourceReference tableA = of("", "tableA", "a");

        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);

        Schema schemaA = Schema.of(col("a", Type.Any, aAst));

        //@formatter:off
        ILogicalPlan expected =
                new Aggregate(
                    tableScan(schemaA, tableA),
                    emptyList(),
                    emptyList());
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(Schema.of(new CoreColumn("a", ResolvedType.of(Type.Any), aAst)));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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

        TableSourceReference tableA = of("", "tableA", "a");

        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);

        Schema schemaA = Schema.of(col("a", Type.Any, aAst));

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    tableScan(schemaA, tableA),
                    asList(
                            new DereferenceExpression(
                                new DereferenceExpression(cre(aAst.rename("log")), "nested", -1, ResolvedType.of(Type.Any)),
                                "level", -1, ResolvedType.of(Type.Any))),
                    false);
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(Schema.of(new CoreColumn("level", ResolvedType.of(Type.Any), aAst.rename("log"))));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");

        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);

        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan expected = new Projection(
                new Join(
                        tableScan(schemaA, tableA),
                        tableScan(schemaB, tableB),
                        Join.Type.INNER,
                        "b",
                        eq(cre(bAst.rename("col1")), cre(aAst.rename("col1"))),
                        null,
                        false
                        ),
                asList(new AliasExpression(new UnresolvedSubQueryExpression(
                        new OperatorFunctionScan(
                            Schema.of(Column.of("output", ResolvedType.table(Schema.of(CoreColumn.of(bAst, ResolvedType.table(schemaB)))))),
                            new Projection(
                                ConstantScan.INSTANCE,
                                asList(ocre("b", bAst, null, ResolvedType.table(schemaB))),
                                false
                            ),
                            "",
                            "object_array",
                            null),
                        asSet(CoreColumn.of(bAst.rename("b"), ResolvedType.table(schemaB))), null), "obj")),
                false);
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(Schema.of(CoreColumn.of("obj", ResolvedType.table(Schema.of(CoreColumn.of(bAst, ResolvedType.table(schemaB)))))));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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

        TableSourceReference tableA = of("", "tableA", "a");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));

        //@formatter:off
        ILogicalPlan expected = new Aggregate(
                tableScan(schemaA, tableA),
                asList(cre(aAst.rename("col"))),
                asList(
                    agg(cre(aAst.rename("col")), true),
                    new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(cre(aAst.rename("col2")))),
                    agg(cre(aAst.rename("col3")), false)));
        //@formatter:on

        //@formatter:off
        assertEquals(Schema.of(
                CoreColumn.of(aAst.rename("col"), ResolvedType.of(Type.Any)),
                new CoreColumn("", ResolvedType.of(Type.Int), "count(a.col2)", false),
                CoreColumn.of(aAst.rename("col3"), ResolvedType.array(Type.Any))
                ), actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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

        TableSourceReference tableA = of("", "tableA", "a");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    tableScan(schemaA, tableA),
                    asList(new FunctionCallExpression(
                            "sys",
                            SystemCatalog.get().getScalarFunction("map"),
                            null,
                            asList(
                               cre(aAst.rename("d")),
                               new LambdaExpression(asList("x"),
                                       lce("x", 0),
                                       new int[] {0})
                            )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre(aAst.rename("d")),
                                   new LambdaExpression(asList("x"),
                                           new DereferenceExpression(lce("x", 0), "value", -1, ResolvedType.of(Type.Any)),
                                           new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre(aAst.rename("a")),
                                   new LambdaExpression(asList("y"),
                                           lce("y", 0),
                                           new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre(aAst.rename("a")),
                                   new LambdaExpression(asList("z"),
                                           new DereferenceExpression(lce("z", 0), "column", -1, ResolvedType.of(Type.Any)),
                                           new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre(aAst.rename("col")),
                                   new LambdaExpression(asList("zz"),
                                           add(new DereferenceExpression(lce("zz", 0), "column", -1, ResolvedType.of(Type.Any)), cre(aAst.rename("value"))),
                                           new int[] {0})
                                ))
                            ));
        //@formatter:on

        //@formatter:off
        assertEquals(Schema.of(
                new CoreColumn("", ResolvedType.of(Type.Any), "map(a.d, x -> x)", false),
                new CoreColumn("", ResolvedType.of(Type.Any), "map(a.d, x -> x.value)", false),
                new CoreColumn("", ResolvedType.of(Type.Any), "map(a.a, y -> y)", false),
                new CoreColumn("", ResolvedType.of(Type.Any), "map(a.a, z -> z.column)", false),
                new CoreColumn("", ResolvedType.of(Type.Any), "map(a.col, zz -> zz.column + a.value)", false)
                ), actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                            tableScan(schemaA, tableA),
                            tableScan(schemaB, tableB),
                            Join.Type.INNER,
                            "b",
                            eq(cre(bAst.rename("col")), cre(aAst.rename("col"))),
                            asSet(),
                            false),
                    asList(new FunctionCallExpression(
                            "sys",
                            SystemCatalog.get().getScalarFunction("map"),
                            null,
                            asList(
                               cre(bAst.rename("b"), ResolvedType.table(schemaB)),
                               new LambdaExpression(asList("x"),
                                       lce("x", 0, ResolvedType.object(schemaB)),
                                       new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre(bAst.rename("b"), ResolvedType.table(schemaB)),
                                   new LambdaExpression(asList("x"),
                                           DereferenceExpression.create(lce("x", 0, ResolvedType.object(schemaB)), QualifiedName.of("col"), null),
                                           new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                    cre(bAst.rename("b"), ResolvedType.table(schemaB)),
                                   new LambdaExpression(asList("z"),
                                           DereferenceExpression.create(lce("z", 0, ResolvedType.object(schemaB)), QualifiedName.of("column"), null),
                                           new int[] {0})
                                )),
                            new FunctionCallExpression(
                                "sys",
                                SystemCatalog.get().getScalarFunction("map"),
                                null,
                                asList(
                                   cre(bAst.rename("b"), ResolvedType.table(schemaB)),
                                   new LambdaExpression(asList("zz"),
                                           add(
                                               DereferenceExpression.create(lce("zz", 0, ResolvedType.object(schemaB)), QualifiedName.of("col2"), null),
                                               cre(aAst.rename("value"))),
                                           new int[] {0})
                                ))
                            ));
        //@formatter:on

        //@formatter:off
        assertEquals(Schema.of(
                new CoreColumn("", ResolvedType.array(ResolvedType.object(schemaB)), "map(b.b, x -> x)", false),       // ValueVector of ObjectVectors
                new CoreColumn("", ResolvedType.array(ResolvedType.of(Type.Any)), "map(b.b, x -> x.col)", false),
                new CoreColumn("", ResolvedType.array(ResolvedType.of(Type.Any)), "map(b.b, z -> z.column)", false),
                new CoreColumn("", ResolvedType.array(ResolvedType.of(Type.Any)), "map(b.b, zz -> zz.col2 + a.value)", false)
                ), actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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

        ILogicalPlan plan = getSchemaResolvedPlan(query);

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

        TableSourceReference tableA = of("", "tableA", "a");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));

        //@formatter:off
        ILogicalPlan expected = 
                new Join(
                    projection(
                            tableScan(schemaA, tableA),
                            asList(new AliasExpression(add(cre(aAst.rename("col")), cre(aAst.rename("col2"))), "col"))),
                    new Filter(
                            tableScan(schemaA, tableA),
                            null,
                            gt(oce("col", 0), cre(aAst.rename("col")))),
                    Join.Type.INNER,
                    null,
                    (IExpression) null,
                    asSet(col("col", Type.Any)),
                    false);
        //@formatter:on

        assertEquals(Schema.of(col("col", Type.Any), col(aAst, Type.Any)), actual.getSchema());

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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
        + "               for object_array) values "
        + "from tableA a "
        + "inner populate join tableB b "
        + "  on b.col1 = a.col1 "
        + "inner populate join tableC c "
        + "  on c.col1 = a.col1 "
        + "inner join stableD d "
        + "  on d.col1 = a.col1 "
        + "";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        TableSourceReference tableC = of("", "tableC", "c");

        TableSourceReference e_b = of("", "b", "b");

        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        ColumnReference cAst = new ColumnReference(tableC, "c", ColumnReference.Type.ASTERISK);
        ColumnReference e_bAst = new ColumnReference(e_b, "b", ColumnReference.Type.ASTERISK);

        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));
        Schema schemaC = Schema.of(col("c", Type.Any, cAst));

        //@formatter:off
        Schema objectArraySchema = Schema.of(
            CoreColumn.of(e_bAst.rename("col2"), ResolvedType.of(Type.Any)),
            CoreColumn.of(cAst.rename("col3"), ResolvedType.array(Type.Any)),
            CoreColumn.of(e_bAst.rename("col4"), ResolvedType.of(Type.Any)),
            CoreColumn.of(aAst.rename("col3"), ResolvedType.of(Type.Any)),
            CoreColumn.of(sTableD.column("col6"), ResolvedType.of(Type.String)),
            CoreColumn.of(cAst.rename("c"), ResolvedType.table(schemaC))
        );
        
        ILogicalPlan expected = 
                projection(
                    new Join( 
                        new Join(
                           new Join(
                               tableScan(schemaA, tableA),
                               tableScan(schemaB, tableB),
                               Join.Type.INNER,
                               "b",
                               eq(cre(bAst.rename("col1")), cre(aAst.rename("col1"))),
                               null,
                               false),
                           tableScan(schemaC, tableC),
                           Join.Type.INNER,
                           "c",
                           eq(cre(cAst.rename("col1")), cre(aAst.rename("col1"))),
                           null,
                           false),
                        tableScan(schemaSTableD, sTableD),
                        Join.Type.INNER,
                        null,
                        eq(cre("col1", sTableD.column("col1"), "col1", ResolvedType.of(Type.Double)), cre(aAst.rename("col1"))),
                        null,
                        false),
                    asList(new AliasExpression(new UnresolvedSubQueryExpression(
                            new OperatorFunctionScan(
                                    Schema.of(Column.of("output", ResolvedType.table(objectArraySchema))),
                                    projection(
                                        new ExpressionScan(
                                            e_b,
                                            Schema.of(CoreColumn.of(e_bAst, ResolvedType.of(Type.Any))),
                                            ocre(bAst.rename("b"), ResolvedType.table(schemaB)),
                                            null),
                                        asList(
                                            cre(e_bAst.rename("col2")),
                                            new DereferenceExpression(ocre("c", cAst.rename("col3"), "c", ResolvedType.table(schemaC)), "col3", -1, ResolvedType.array(Type.Any)),
                                            cre(e_bAst.rename("col4")),
                                            ocre(aAst.rename("col3")),
                                            ocre("col6", sTableD.column("col6"), "col6", ResolvedType.of(Type.String)),
                                            ocre("c", cAst.rename("c"), "c", ResolvedType.table(schemaC))
                                        )),
                                    "",
                                    "object_array",
                                    null),
                                    asSet(
                                        CoreColumn.of(bAst.rename("b"), ResolvedType.table(schemaB)),
                                        CoreColumn.of(cAst.rename("col3"), ResolvedType.table(schemaC)),
                                        CoreColumn.of(aAst.rename("col3"), ResolvedType.of(Type.Any)),
                                        CoreColumn.of(sTableD.column("col6"), ResolvedType.of(Type.String)),     // static schema col6 was chosen instead of asterisk
                                        CoreColumn.of(cAst.rename("c"), ResolvedType.table(schemaC))
                                        ),
                                    null),
                             "values")));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
                .isEqualTo(Schema.of(CoreColumn.of("values", ResolvedType.table(objectArraySchema))));

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
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

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference range = of("", "range", "r");

        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);

        ColumnReference rValue = range.column("Value");

        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaRange = Schema.of(col("Value", Type.Int, rValue));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    new TableFunctionScan(range, schemaRange, asList(intLit(1), ocre(aAst.rename("col2"))), emptyList(), null),
                    Join.Type.INNER,
                    null,
                    (IExpression) null,
                    asSet(col(aAst.rename("col2"), Type.Any)),
                    false),
                asList(cre(aAst.rename("col")), cre(rValue, ResolvedType.of(Type.Int)))
                );
        //@formatter:on

        assertEquals(Schema.of(col(aAst.rename("col"), Type.Any), col(rValue, Type.Int)), actual.getSchema());

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
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

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference range = of("", "range", "r");

        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);

        ColumnReference rValue = range.column("Value");

        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaRange = Schema.of(col("Value", Type.Int, rValue));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    new TableFunctionScan(range, schemaRange, asList(intLit(1), intLit(10)), emptyList(), null),
                    Join.Type.INNER,
                    null,
                    (IExpression) null,
                    asSet(),
                    false),
                asList(cre(aAst.rename("col")), cre("Value", rValue, "Value", ResolvedType.of(Type.Int)))
                );
        //@formatter:on

        assertEquals(Schema.of(col(aAst.rename("col"), Type.Any), col(rValue, Type.Int)), actual.getSchema());

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
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

        TableSourceReference rangeA = new TableSourceReference("", QualifiedName.of("range"), "a");
        TableSourceReference rangeR1 = new TableSourceReference("", QualifiedName.of("range"), "r1");
        TableSourceReference rangeR2 = new TableSourceReference("", QualifiedName.of("range"), "r2");

        ColumnReference colValueA = rangeA.column("Value");
        ColumnReference colValueR1 = rangeR1.column("Value");
        ColumnReference colValueR2 = rangeR2.column("Value");

        Schema schemaRangeA = Schema.of(col("Value", Type.Int, colValueA));
        Schema schemaRangeR1 = Schema.of(col("Value", Type.Int, colValueR1));
        Schema schemaRangeR2 = Schema.of(col("Value", Type.Int, colValueR2));

        //@formatter:off
        ILogicalPlan expected = 
                new Join(
                        new Join(
                            new TableFunctionScan(rangeA, schemaRangeA, asList(intLit(1), intLit(10)), emptyList(), null),
                            new TableFunctionScan(rangeR1, schemaRangeR1, asList(intLit(1), ocre(colValueA, 0, ResolvedType.of(Type.Int))), emptyList(), null),
                            Join.Type.INNER,
                            null,
                            (IExpression) null,
                            asSet(col(colValueA, Type.Int)),
                            false),
                        new TableFunctionScan(rangeR2, schemaRangeR2, asList(intLit(1), ocre(colValueR1, 1, ResolvedType.of(Type.Int))), emptyList(), null),
                    Join.Type.INNER,
                    null,
                    (IExpression) null,
                    asSet(col(colValueR1, Type.Int)),
                    false);
        //@formatter:on

        assertEquals(SchemaUtils.concat(schemaRangeA, SchemaUtils.concat(schemaRangeR1, schemaRangeR2)), actual.getSchema());

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
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

        TableSourceReference rangeA = new TableSourceReference("", QualifiedName.of("range"), "a");
        TableSourceReference rangeR1 = new TableSourceReference("", QualifiedName.of("range"), "r1");
        TableSourceReference rangeR2 = new TableSourceReference("", QualifiedName.of("range"), "r2");

        ColumnReference colValueA = rangeA.column("Value");
        ColumnReference colValueR1 = rangeR1.column("Value");
        ColumnReference colValueR2 = rangeR2.column("Value");

        Schema schemaRangeA = Schema.of(col("Value", Type.Int, colValueA));
        Schema schemaRangeR1 = Schema.of(col("Value", Type.Int, colValueR1));
        Schema schemaRangeR2 = Schema.of(col("Value", Type.Int, colValueR2));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                            new Join(
                                new TableFunctionScan(rangeA, schemaRangeA, asList(intLit(1), intLit(10)), emptyList(), null),
                                new TableFunctionScan(rangeR1, schemaRangeR1, asList(intLit(1), ocre(colValueA, 0, ResolvedType.of(Type.Int))), emptyList(), null),
                                Join.Type.INNER,
                                null,
                                (IExpression) null,
                                asSet(col(colValueA, Type.Int)),
                                false),
                            new TableFunctionScan(rangeR2, schemaRangeR2, asList(intLit(1), ocre(colValueR1, 1, ResolvedType.of(Type.Int))), emptyList(), null),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(col(colValueR1, Type.Int)),
                        false),
                asList(cre(colValueR1, 1, ResolvedType.of(Type.Int))));
        //@formatter:on

        assertEquals(schemaRangeR1, actual.getSchema());

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
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

        /*
         * value index r1 value index value index
         * 
         * 
         * r1 TupleVector Value: ValueVector Index: ValueVector
         * 
         * 
         * 
         */

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference rangeA = new TableSourceReference("", QualifiedName.of("range"), "a");
        TableSourceReference rangeR1 = new TableSourceReference("", QualifiedName.of("range"), "r1");
        TableSourceReference rangeR2 = new TableSourceReference("", QualifiedName.of("range"), "r2");

        ColumnReference colValueA = rangeA.column("Value");
        ColumnReference colValueR1 = rangeR1.column("Value");
        ColumnReference colValueR2 = rangeR2.column("Value");

        Schema schemaRangeA = Schema.of(col("Value", Type.Int, colValueA));
        Schema schemaRangeR1 = Schema.of(col("Value", Type.Int, colValueR1));
        Schema schemaRangeR2 = Schema.of(col("Value", Type.Int, colValueR2));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                            new Join(
                                new TableFunctionScan(rangeA, schemaRangeA, asList(intLit(1), intLit(10)), emptyList(), null),
                                new TableFunctionScan(rangeR1, schemaRangeR1, asList(intLit(1), ocre(colValueA, 0, ResolvedType.of(Type.Int))), emptyList(), null),
                                Join.Type.INNER,
                                "r1",
                                (IExpression) null,
                                asSet(col(colValueA, Type.Int)),
                                false),
                            new TableFunctionScan(rangeR2, schemaRangeR2, asList(
                                    intLit(1),
                                    new DereferenceExpression(ocre("r1", colValueR1, 1, ResolvedType.table(schemaRangeR1)), "Value", 0, ResolvedType.array(ResolvedType.of(Type.Int)))),
                                    emptyList(), null),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(new CoreColumn("Value", ResolvedType.table(schemaRangeR1), colValueR1)),
                        false),
                    asList(new DereferenceExpression(cre("r1", colValueR1, 1, ResolvedType.table(schemaRangeR1)), "Value", 0, ResolvedType.array(ResolvedType.of(Type.Int)))));

        assertEquals(
                Schema.of(
                        new CoreColumn("Value", ResolvedType.array(ResolvedType.of(Type.Int)), colValueR1)),
                actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
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

        TableSourceReference rangeA = new TableSourceReference("", QualifiedName.of("range"), "a");
        TableSourceReference rangeR1 = new TableSourceReference("", QualifiedName.of("range"), "r1");
        TableSourceReference rangeR2 = new TableSourceReference("", QualifiedName.of("range"), "r2");

        ColumnReference colValueA = rangeA.column("Value");
        ColumnReference colValueR1 = rangeR1.column("Value");
        ColumnReference colValueR2 = rangeR2.column("Value");

        Schema schemaRangeA = Schema.of(col("Value", Type.Int, colValueA));
        Schema schemaRangeR1 = Schema.of(col("Value", Type.Int, colValueR1));
        Schema schemaRangeR2 = Schema.of(col("Value", Type.Int, colValueR2));

        //@formatter:off
        ILogicalPlan expected = 
                new Join(
                        new Join(
                            new TableFunctionScan(rangeA, schemaRangeA, asList(intLit(1), intLit(10)), emptyList(), null),
                            new TableFunctionScan(rangeR1, schemaRangeR1, asList(intLit(1), ocre(colValueA, 0, ResolvedType.of(Type.Int))), emptyList(), null),
                            Join.Type.INNER,
                            "r1",
                            (IExpression) null,
                            asSet(col(colValueA, Type.Int)),
                            false),
                        new TableFunctionScan(rangeR2, schemaRangeR2, asList(
                                intLit(1),
                                new DereferenceExpression(ocre("r1", colValueR1, 1, ResolvedType.table(schemaRangeR1)), "Value", 0, ResolvedType.array(ResolvedType.of(Type.Int)))),
                                emptyList(), null),
                    Join.Type.INNER,
                    null,
                    (IExpression) null,
                    asSet(new CoreColumn("Value", ResolvedType.table(schemaRangeR1), colValueR1)),
                    false);
        //@formatter:on

        //@formatter:off
        assertEquals(Schema.of(
                // All a
                schemaRangeA.getColumns().get(0),
                // Populate r1
                new CoreColumn("r1", ResolvedType.table(schemaRangeR1), rangeR1.column("r1")),
                // All r2
                schemaRangeR2.getColumns().get(0)
                ), actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
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

        TableSourceReference rangeA = new TableSourceReference("", QualifiedName.of("range"), "a");
        TableSourceReference rangeR1 = new TableSourceReference("", QualifiedName.of("range"), "r1");
        TableSourceReference rangeR2 = new TableSourceReference("", QualifiedName.of("range"), "r2");

        ColumnReference colValueA = rangeA.column("Value");
        ColumnReference colValueR1 = rangeR1.column("Value");
        ColumnReference colValueR2 = rangeR2.column("Value");

        Schema schemaRangeA = Schema.of(col("Value", Type.Int, colValueA));
        Schema schemaRangeR1 = Schema.of(col("Value", Type.Int, colValueR1));
        Schema schemaRangeR2 = Schema.of(col("Value", Type.Int, colValueR2));

        //@formatter:off
        ILogicalPlan expected = 
                new Join(
                        new Join(
                            new TableFunctionScan(rangeA, schemaRangeA, asList(intLit(1), intLit(10)), emptyList(), null),
                            new TableFunctionScan(rangeR1, schemaRangeR1, asList(intLit(1), ocre(colValueA, 0, ResolvedType.of(Type.Int))), emptyList(), null),
                            Join.Type.INNER,
                            "r1",
                            (IExpression) null,
                            asSet(col(colValueA, Type.Int)),
                            false),
                        new TableFunctionScan(rangeR2, schemaRangeR2, asList(
                                intLit(1),
                                new DereferenceExpression(ocre("r1", colValueR1, 1, ResolvedType.table(schemaRangeR1)), "Value", 0, ResolvedType.array(ResolvedType.of(Type.Int)))),
                                emptyList(), null),
                    Join.Type.INNER,
                    "r2",
                    (IExpression) null,
                    asSet(new CoreColumn("Value", ResolvedType.table(schemaRangeR1), colValueR1)),
                    false);
        //@formatter:on

        //@formatter:off
        assertEquals(Schema.of(
                // All a
                schemaRangeA.getColumns().get(0),
                // Populate r1
                new CoreColumn("r1", ResolvedType.table(schemaRangeR1), rangeR1.column("r1")),
                // Populate r2
                new CoreColumn("r2", ResolvedType.table(schemaRangeR2), rangeR2.column("r2"))
                ), actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
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

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    tableScan(schemaB, tableB),
                    Join.Type.INNER,
                    "b",
                    eq(cre(bAst.rename("col2")), cre(aAst.rename("col2"))),
                    asSet(),
                    false),
                asList(cre(aAst.rename("col")), new DereferenceExpression(cre("b", bAst.rename("col"), "b", ResolvedType.table(schemaB)), "col", -1, ResolvedType.array(Type.Any)))
                );
        //@formatter:on

        //@formatter:off
        assertEquals(Schema.of(
                col(aAst.rename("col"), Type.Any), 
                new CoreColumn("col", ResolvedType.array(ResolvedType.of(Type.Any)), bAst.rename("col"))),
                actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
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

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    tableScan(schemaB, tableB),
                    Join.Type.INNER,
                    "b",
                    eq(cre(bAst.rename("col2")), cre(aAst.rename("col2"))),
                    asSet(),
                    false),
                asList(cre(aAst.rename("col")), cre(bAst.rename("b"), ResolvedType.table(schemaB)))
                );
        //@formatter:on

        assertEquals(Schema.of(col(aAst.rename("col"), Type.Any), new CoreColumn("b", ResolvedType.table(schemaB), bAst.rename("b"))), actual.getSchema());

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    tableScan(schemaB, tableB),
                    Join.Type.INNER,
                    "b",
                    eq(cre(bAst.rename("col2")), cre(aAst.rename("col2"))),
                    asSet(),
                    false),
                asList(new ColumnExpression("b", null, ResolvedType.table(schemaB), bAst, -1, false, -1))
                );
        //@formatter:on

        assertEquals(Schema.of(new CoreColumn("b", ResolvedType.table(schemaB), bAst)), actual.getSchema());

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_expression_scan_on_root_missing_table_column()
    {
        String query = "select col from (a) for object_array";

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
        // Here we are accessing what seems to be outer columns (b.col, b.col2 in sub query expression) but we have an over clause
        // here which makes this a plain column access since we are going to scan the tuple vector in alias b as 
        // plain table source
        String query = ""
                + "select a.col, b.col bOuterCol, ( select b.col, b.col2, a.col3 from (b) b for object_array ) "
                + "from tableA a "
                + "inner populate join tableB b "
                + "  on b.col2 = a.col2 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        TableSourceReference e_b = of("", "b", "b");

        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        ColumnReference open_tableAst = new ColumnReference(e_b, "b", ColumnReference.Type.ASTERISK);

        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        Schema objectArraySchema = Schema.of(
            CoreColumn.of(open_tableAst.rename("col"), ResolvedType.of(Type.Any)),
            CoreColumn.of(open_tableAst.rename("col2"), ResolvedType.of(Type.Any)),
            CoreColumn.of(aAst.rename("col3"), ResolvedType.of(Type.Any))
        );
        
        ILogicalPlan expected =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    tableScan(schemaB, tableB),
                    Join.Type.INNER,
                    "b",
                    eq(cre(bAst.rename("col2")), cre(aAst.rename("col2"))),
                    asSet(),
                    false),
                asList(cre(aAst.rename("col")), new AliasExpression(
                        new DereferenceExpression(cre("b", bAst.rename("col"), "b", ResolvedType.table(schemaB)), "col", -1, ResolvedType.array(Type.Any)), "bOuterCol"),
                            new UnresolvedSubQueryExpression(
                                new OperatorFunctionScan(
                                    Schema.of(Column.of("output", ResolvedType.table(objectArraySchema))),
                                    projection(
                                            new ExpressionScan(
                                                e_b,
                                                Schema.of(CoreColumn.of(open_tableAst, ResolvedType.of(Type.Any))),
                                                ocre(bAst.rename("b"), ResolvedType.table(schemaB)),
                                                null),
                                            asList(
                                                cre(open_tableAst.rename("col")),
                                                cre(open_tableAst.rename("col2")),
                                                ocre(aAst.rename("col3"))
                                            )),
                                    "",
                                    "object_array",
                                    null),
                            asSet(new CoreColumn("b", ResolvedType.table(Schema.of(col(bAst, Type.Any))), bAst.rename("b")), col(aAst.rename("col3"), Type.Any)),
                        null)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_populate_join_with_expression_scan_schema_full()
    {
//        //@formatter:off
//        String query = ""
//                + "select a.col, b.col bOuterCol, ( select b.col, b.col2, a.col3 from open_table(b) for object_array ) "
//                + "from tableA a "
//                + "inner populate join tableB b "
//                + "  on b.col2 = a.col2 ";
//        //@formatter:on

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        TableSourceReference e_b = of("", "b", "b");

        ColumnReference aCol = new ColumnReference(tableA, "col", ColumnReference.Type.REGULAR);
        ColumnReference aCol2 = new ColumnReference(tableA, "col2", ColumnReference.Type.REGULAR);
        ColumnReference aCol3 = new ColumnReference(tableA, "col3", ColumnReference.Type.REGULAR);

        ColumnReference b = new ColumnReference(tableB, "b", ColumnReference.Type.REGULAR);
        ColumnReference bCol = new ColumnReference(tableB, "col", ColumnReference.Type.REGULAR);
        ColumnReference bCol2 = new ColumnReference(tableB, "col2", ColumnReference.Type.REGULAR);

        ColumnReference obCol = new ColumnReference(e_b, "col", ColumnReference.Type.REGULAR);
        ColumnReference obCol2 = new ColumnReference(e_b, "col2", ColumnReference.Type.REGULAR);

        Schema schemaA = Schema.of(col(aCol, Type.Int), col(aCol2, Type.String), col(aCol3, Type.Float));
        Schema schemaB = Schema.of(col(bCol, Type.Boolean), col(bCol2, Type.Int));

        //@formatter:off
        ILogicalPlan plan =  projection(
                new Join(
                    tableScan(schemaA, tableA),
                    tableScan(schemaB, tableB),
                    Join.Type.INNER,
                    "b",
                    e("b.col2 = a.col2"),
                    asSet(),
                    false),
                asList(e("a.col"), new AliasExpression(e("b.col"), "bOuterCol"), new UnresolvedSubQueryExpression(
                        new OperatorFunctionScan(
                                Schema.of(col("output", Type.Any)),
                                projection(
                                        new ExpressionScan(
                                            e_b,
                                            Schema.EMPTY,
                                            e("b"),
                                            null),
                                        asList(
                                            e("b.col"),
                                            e("b.col2"),
                                            e("a.col3")
                                        )),
                                "",
                                "object_array",
                                null),
                        asSet(),
                        null)));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        Schema objectArraySchema = Schema.of(
            CoreColumn.of(obCol, ResolvedType.of(Type.Boolean)),
            CoreColumn.of(obCol2, ResolvedType.of(Type.Int)),
            CoreColumn.of(aCol3, ResolvedType.of(Type.Float))
        );
        
        ILogicalPlan expected =
                projection(
                    new Join(
                        tableScan(schemaA, tableA),
                        tableScan(schemaB, tableB),
                        Join.Type.INNER,
                        "b",
                        eq(cre(bCol2, 4, ResolvedType.of(Type.Int)), cre(aCol2, 1, ResolvedType.of(Type.String))),
                        asSet(),
                        false),
                asList(cre(aCol, 0, ResolvedType.of(Type.Int)),
                       new AliasExpression(new DereferenceExpression(cre("b", bCol, 3, ResolvedType.table(schemaB)), "col", 0, ResolvedType.array(ResolvedType.of(Type.Boolean))),
                            "bOuterCol"),
                       new UnresolvedSubQueryExpression(
                           new OperatorFunctionScan(
                                Schema.of(Column.of("output", ResolvedType.table(objectArraySchema))),
                                projection(
                                        new ExpressionScan(
                                            e_b,
                                            Schema.of(CoreColumn.of(obCol, ResolvedType.of(Type.Boolean)), CoreColumn.of(obCol2, ResolvedType.of(Type.Int))),
                                            ocre("b", b, 3, ResolvedType.table(schemaB)),
                                            null),
                                        asList(
                                            cre(obCol, 0, ResolvedType.of(Type.Boolean)),
                                            cre(obCol2, 1, ResolvedType.of(Type.Int)),
                                            ocre(aCol3, 2, ResolvedType.of(Type.Float))
                                        )),
                                "",
                                "object_array",
                                null),
                        asSet(new CoreColumn("b", ResolvedType.table(schemaB), tableB.column("b")), col(aCol3, Type.Float)),
                        null)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_populate_join_schema_full()
    {
        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        ColumnReference aCol = new ColumnReference(tableA, "col", ColumnReference.Type.REGULAR);
        ColumnReference aCol2 = new ColumnReference(tableA, "col2", ColumnReference.Type.REGULAR);
        ColumnReference bCol = new ColumnReference(tableB, "col", ColumnReference.Type.REGULAR);
        ColumnReference bCol2 = new ColumnReference(tableB, "col2", ColumnReference.Type.REGULAR);
        Schema schemaA = Schema.of(col(aCol, Type.Int), col(aCol2, Type.Double));
        Schema schemaB = Schema.of(col(bCol, Type.String), col(bCol2, Type.Double));

        //@formatter:off
        ILogicalPlan plan =  projection(
                new Join(
                        tableScan(schemaA, tableA),
                        tableScan(schemaB, tableB),
                        Join.Type.INNER,
                        "b",
                        e("b.col2 = a.col2"),
                        asSet(),
                        false),
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
                    eq(cre(bCol2, 3, ResolvedType.of(Type.Double)), cre(aCol2, 1, ResolvedType.of(Type.Double))),
                    asSet(),
                    false),
                asList(
                        cre(aCol, 0, ResolvedType.of(Type.Int)),
                        new DereferenceExpression(cre("b", bCol, 2, ResolvedType.table(schemaB)), "col", 0, ResolvedType.array(ResolvedType.of(Type.String))))
                );
        //@formatter:on

        assertEquals(Schema.of(col(aCol, Type.Int), new CoreColumn("col", ResolvedType.array(ResolvedType.of(Type.String)), bCol)), actual.getSchema());

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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
                + ") values "
                + "from tableA a ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of("", "tableA", "a");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));

        //@formatter:off
        ILogicalPlan expected =  projection(
                tableScan(schemaA, tableA),
                asList(cre(aAst.rename("col")), new AliasExpression(new UnresolvedSubQueryExpression(
                        projection(
                            ConstantScan.INSTANCE,
                            asList(ocre(aAst.rename("col2")))),
                        asSet(col(aAst.rename("col2"), Type.Any)),
                        null), "values"))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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
        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        ColumnReference aCol1 = tableA.column("col1");
        ColumnReference aCol3 = tableA.column("col3");
        ColumnReference bCol1 = tableB.column("col1");
        ColumnReference bCol2 = tableB.column("col2");
        Schema schemaA = Schema.of(col(aCol1, Type.Int), col(aCol3, Type.Int));
        Schema schemaB = Schema.of(col(bCol1, Type.Int), col(bCol2, Type.Int));

        //@formatter:off
        ILogicalPlan plan = projection(
                tableScan(schemaA, tableA), 
                asList(e("col1"), new AliasExpression(
                        new UnresolvedSubQueryExpression(
                            projection(
                                tableScan(schemaB, tableB),
                                asList(e("b.col1"), e("col3"))),
                        null),
                        "vals")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    tableScan(schemaA, tableA),
                    asList(cre(aCol1, 0, ResolvedType.of(Type.Int)), new AliasExpression(
                        new UnresolvedSubQueryExpression(
                            projection(
                                tableScan(schemaB, tableB),
                                asList(cre(bCol1, 0, ResolvedType.of(Type.Int)), ocre(aCol3, 1, ResolvedType.of(Type.Int)))),
                            asSet(col(aCol3, Type.Int)),
                            null),
                            "vals"))
                );
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(expected);

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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
                + ") values "
                + "from tableA a ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan expected =  projection(
                tableScan(schemaA, tableA),
                asList(cre(aAst.rename("col")), new AliasExpression(new UnresolvedSubQueryExpression(
                        projection(
                            tableScan(schemaB, tableB),
                            asList(cre(bAst.rename("col2")))),
                        null), "values"))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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
                + ") values "
                + "from tableA a ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        TableSourceReference tableC = of("", "tableC", "c");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        ColumnReference cAst = new ColumnReference(tableC, "c", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));
        Schema schemaC = Schema.of(col("c", Type.Any, cAst));

        //@formatter:off
        ILogicalPlan expected =  projection(
                tableScan(schemaA, tableA),
                asList(cre(aAst.rename("col")), new AliasExpression(
                        new UnresolvedSubQueryExpression(
                            projection(
                                new Filter(
                                    tableScan(schemaB, tableB),
                                    null,
                                    gt(cre(bAst.rename("col3")), ocre(aAst.rename("col4")))),
                                asList(new AliasExpression(
                                        new UnresolvedSubQueryExpression(
                                                projection(
                                                    new Filter(
                                                        tableScan(schemaC, tableC),
                                                        null,
                                                        and(gt(cre(cAst.rename("col1")), ocre(bAst.rename("col2"))), gt(cre(cAst.rename("col2")), ocre(aAst.rename("col3"))))),
                                                    asList(cre(cAst.rename("col5")))),
                                            asSet(col(bAst.rename("col2"), Type.Any), col(aAst.rename("col3"), Type.Any)),
                                            null),
                                       "keys"))),
                                asSet(col(bAst.rename("col2"), Type.Any), col(aAst.rename("col3"), Type.Any), col(aAst.rename("col4"), Type.Any)),
                            null), 
                        "values"))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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

        TableSourceReference tableA = of("es", "tableA", "a");
        TableSourceReference tableB_a = of("es", "tableB", "a");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB_a, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB_a = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan plan = 
                new Join(
                        tableScan(schemaA, tableA),
                        subQuery(
                            new Filter(
                                tableScan(schemaB_a, tableB_a),
                                null,
                                e("a.col > 10")),
                        "x"),
                        Join.Type.INNER,
                        null,
                        e("x.col2 = a.col2"),
                        asSet(),
                        false);
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                        new Join(
                            tableScan(schemaA, tableA),
                            new Filter(
                                    tableScan(schemaB_a, tableB_a),
                                    null,
                                    gt(cre(bAst.rename("col")), intLit(10))),
                            Join.Type.INNER,
                            null,
                            eq(cre(bAst.rename("col2")), cre(aAst.rename("col2"))),
                            asSet(),
                            false);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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

        TableSourceReference tableA = of("es", "tableA", "a");
        TableSourceReference tableB = of("es", "tableB", "a");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan plan = 
                new Join(
                        tableScan(schemaA, tableA),
                        subQuery(
                            new Filter(
                                tableScan(schemaB, tableB),
                                null,
                                e("a.col = a.col")),
                        "a"),
                        Join.Type.INNER,
                        null,
                        e("a.col2 = a.col2"),
                        asSet(),
                        false);
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

        TableSourceReference tableA = of("es", "tableA", "a");
        TableSourceReference tableB = of("es", "tableB", "a");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan plan = 
                new Join(
                        tableScan(schemaA, tableA),
                        tableScan(schemaB, tableB),
                        Join.Type.INNER,
                        null,
                        e("a.col2 = a.col2"),
                        asSet(),
                        false);
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

        TableSourceReference tableA = of("es", "tableA", "a");
        TableSourceReference tableB = of("", "range", "a");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan plan = 
                new Join(
                        tableScan(schemaA, tableA),
                        new TableFunctionScan(tableB, schemaB, emptyList(), emptyList(), null),
                        Join.Type.LEFT,
                        null,
                        (IExpression) null,
                        asSet(),
                        false);
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

        TableSourceReference tableA = of("es", "tableA", "a");
        TableSourceReference tableB = of("es", "tableB", "b");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

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
                                false),
                            asList(e("a.col"))),
                        "x"),
                    asList(e("x.col")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                    projection(
                        new Join(
                            tableScan(schemaA, tableA),
                            tableScan(schemaB, tableB),
                            Join.Type.LEFT,
                            null,
                            (IExpression) null,
                            asSet(),
                            false),
                        asList(cre(aAst.rename("col"))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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

        TableSourceReference tableA = of("es", "tableA", "a");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                            projection(
                                tableScan(schemaA, tableA),
                                asList(new AliasExpression(e("a.col1 + a.col2"), "col"), e("a.col3"))),
                        "x"),
                    asList(e("x.col")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                    projection(
                        tableScan(schemaA, tableA),
                        asList(new AliasExpression(add(cre(aAst.rename("col1")), cre(aAst.rename("col2"))), "col")));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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

        TableSourceReference tableA = of("es", "tableA", "a");
        TableSourceReference tableB = of("es", "tableB", "b");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

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
                            "x"),
                            Join.Type.INNER,
                            null,
                            (IExpression) null,
                            asSet(),
                            false),
                    null,
                    e("x.col2 = a.col2"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        tableScan(schemaA, tableA),
                        new Filter(
                            tableScan(schemaB, tableB),
                            null,
                            eq(cre(bAst.rename("col")), ocre(aAst.rename("col")))),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(col(aAst.rename("col"), Type.Any)),
                        false),
                    null,
                    eq(cre(bAst.rename("col2")), cre(aAst.rename("col2"))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

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
                            "x"),
                            Join.Type.INNER,
                            null,
                            (IExpression) null,
                            asSet(),
                            false),
                    null,
                    e("x.col2 = a.col2"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        tableScan(schemaA, tableA),
                        projection(
                            new Filter(
                                tableScan(schemaB, tableB),
                                null,
                                eq(cre(bAst.rename("col")), ocre(aAst.rename("col")))),
                            asList(cre(bAst.rename("col2")), ocre(aAst.rename("col3")))),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(col(aAst.rename("col"), Type.Any), col(aAst.rename("col3"), Type.Any)),
                        false),
                    null,
                    eq(cre(bAst.rename("col2")), cre(aAst.rename("col2"))));
        //@formatter:on

        assertEquals(Schema.of(col(aAst, Type.Any), col(bAst.rename("col2"), Type.Any), col(aAst.rename("col3"), Type.Any)), actual.getSchema());

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        ColumnReference aCol = new ColumnReference(tableA, "col", ColumnReference.Type.REGULAR);
        ColumnReference aCol2 = new ColumnReference(tableA, "col2", ColumnReference.Type.REGULAR);
        ColumnReference aCol3 = new ColumnReference(tableA, "col3", ColumnReference.Type.REGULAR);
        ColumnReference bCol = new ColumnReference(tableB, "col", ColumnReference.Type.REGULAR);
        ColumnReference bCol2 = new ColumnReference(tableB, "col2", ColumnReference.Type.REGULAR);
        Schema schemaA = Schema.of(col(aCol, Type.String), col(aCol3, Type.Int), col(aCol2, Type.Any));
        Schema schemaB = Schema.of(col(bCol, Type.Int), col(bCol2, Type.Float));

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
                            "x"),
                            Join.Type.INNER,
                            null,
                            (IExpression) null,
                            asSet(),
                            false),
                    null,
                    e("x.col2 = a.col2"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        tableScan(schemaA, tableA),
                        projection(
                            new Filter(
                                tableScan(schemaB, tableB),
                                null,
                                eq(cre(bCol, 0, ResolvedType.of(Type.Int)), ocre(aCol, 0, ResolvedType.of(Type.String)))),
                            asList(cre(bCol2, 1, ResolvedType.of(Type.Float)), ocre(aCol3, 1, ResolvedType.of(Type.Int)))),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(col(aCol, Type.String), col(aCol3, Type.Int)),
                        false),
                    null,
                    eq(cre(bCol2, 3, ResolvedType.of(Type.Float)), cre(aCol2, 2)));
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .isEqualTo(
                //@formatter:off
                    Schema.of(
                        // 1:st all table a columns
                        col(aCol, Type.String),
                        col(aCol3, Type.Int),
                        col(aCol2, Type.Any),

                        // ... then all sub query columns
                        col(bCol2, Type.Float),
                        col(aCol3, Type.Int))
                    //@formatter:on
                );

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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

        TableSourceReference tableA = of("", "tableA", "a");
        TableSourceReference tableB = of("", "tableB", "b");
        TableSourceReference tableC = of("", "tableC", "c");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        ColumnReference cAst = new ColumnReference(tableC, "c", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));
        Schema schemaC = Schema.of(col("c", Type.Any, cAst));
        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Join(
                        tableScan(schemaA, tableA),
                        projection(
                            new Filter(
                                new Join(
                                    tableScan(schemaB, tableB),
                                    projection(
                                        new Filter(
                                            tableScan(schemaC, tableC),
                                            null,
                                            and(eq(cre(cAst.rename("col")), ocre(bAst.rename("col"))), eq(cre(cAst.rename("col2")), ocre(aAst.rename("col2"))))),
                                        asList(cre(cAst.rename("col3")), cre(cAst))),
                                    Join.Type.INNER,
                                    null,
                                    (IExpression) null,
                                    asSet(col(bAst.rename("col"), Type.Any), col(aAst.rename("col2"), Type.Any)),
                                    false),
                                null,
                                and(eq(cre(bAst.rename("col")), ocre(aAst.rename("col"))), eq(cre(cAst.rename("col3")), cre(bAst.rename("col3"))))),
                        asList(cre(bAst.rename("col2")), cre(bAst), cre(cAst.rename("col3")), cre(cAst))),
                        Join.Type.INNER,
                        null,
                        (IExpression) null,
                        asSet(col(bAst.rename("col"), Type.Any), col(aAst.rename("col2"), Type.Any), col(aAst.rename("col"), Type.Any)),
                        false),
                    null,
                    eq(cre(bAst.rename("col2")), cre(aAst.rename("col2"))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        //@formatter:off
        assertEquals(Schema.of(
                col(aAst, Type.Any),
                col(bAst.rename("col2"), Type.Any),
                col(bAst, Type.Any),
                col(cAst.rename("col3"), Type.Any),
                col(cAst, Type.Any)), actual.getSchema());
        //@formatter:on
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

        TableSourceReference tableA = of("es", "tableA", "a");
        TableSourceReference tableB = of("es", "tableB", "b");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

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
                            false),
                        "x"),
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

        TableSourceReference tableA = of("es", "tableA", "a");
        TableSourceReference tableB = of("es", "tableB", "b");

        ColumnReference aCol = new ColumnReference(tableA, "col", ColumnReference.Type.REGULAR);
        ColumnReference aCol1 = new ColumnReference(tableA, "col1", ColumnReference.Type.REGULAR);
        ColumnReference bCol = new ColumnReference(tableB, "col", ColumnReference.Type.REGULAR);

        Schema schemaA = Schema.of(col(aCol, Type.Any), col(aCol1, Type.Any));
        Schema schemaB = Schema.of(col(bCol, Type.Any));

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
                            false),
                        "x"),
                    asList(e("x.col1")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        tableScan(schemaA, tableA),
                        tableScan(schemaB, tableB),
                        Join.Type.INNER,
                        null,
                        eq(cre(bCol, 2), cre(aCol, 0)),
                        asSet(),
                        false),
                    asList(cre(aCol1, 1)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
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

        TableSourceReference tableA = of("es", "tableA", "a");
        TableSourceReference tableB = of("es", "tableB", "b");

        ColumnReference aCol = new ColumnReference(tableA, "col", ColumnReference.Type.REGULAR);
        ColumnReference aCol1 = new ColumnReference(tableA, "col1", ColumnReference.Type.REGULAR);
        ColumnReference bCol = new ColumnReference(tableB, "col", ColumnReference.Type.REGULAR);

        Schema schemaA = Schema.of(col(aCol, Type.Any), col(aCol1, Type.Any));
        Schema schemaB = Schema.of(col(bCol, Type.Any));

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
                            false),
                        "x"),
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

        TableSourceReference tableA = of("es", "tableA", "a");
        TableSourceReference tableB = of("es", "tableB", "b");
        ColumnReference aAst = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaA = Schema.of(col("a", Type.Any, aAst));
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan plan = 
                new Join(
                    tableScan(schemaA, tableA),
                    subQuery(
                        tableScan(schemaB, tableB),
                    "x"),
                    Join.Type.INNER,
                    null,
                    e("x.col = a.col"),
                    asSet(),
                    false);
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Join(
                        tableScan(schemaA, tableA),
                        tableScan(schemaB, tableB),
                    Join.Type.INNER,
                    null,
                    eq(cre(bAst.rename("col")), cre(aAst.rename("col"))),
                    asSet(),
                    false);
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

        TableSourceReference tableB = of("es", "tableB", "b");
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaB = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan plan = 
                new Filter(
                    subQuery(
                        new Filter(
                            tableScan(schemaB, tableB),
                            null,
                            e("b.col1 > 10")),
                    "x"),
                    null,
                    e("x.col > 20"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    tableScan(schemaB, tableB),
                    null,
                    and(gt(cre(bAst.rename("col1")), new LiteralIntegerExpression(10)), gt(cre(bAst.rename("col")), new LiteralIntegerExpression(20))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(schemaB, actual.getSchema());
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

        TableSourceReference tableB = of("es", "tableB", "b");
        ColumnReference bRef = new ColumnReference(tableB, "b", ColumnReference.Type.REGULAR);
        Column col1 = col(bRef.rename("col1"), Type.Float);
        Column col = col(bRef.rename("col"), Type.Double);
        Schema schemaB = Schema.of(col1, col);

        //@formatter:off
        ILogicalPlan plan = 
                new Filter(
                    subQuery(
                        new Filter(
                            tableScan(schemaB, tableB),
                            null,
                            e("b.col1 > 10")),
                    "x"),
                    null,
                    e("x.col > 20"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    tableScan(schemaB, tableB),
                    null,
                    // b.col1 > 10 AND b.col > 20
                     and(
                         gt(
                             cre(SchemaUtils.getColumnReference(col1), 0, ResolvedType.of(Type.Float)),
                             intLit(10)),
                         gt(
                             cre(SchemaUtils.getColumnReference(col), 1, ResolvedType.of(Type.Double)),
                             intLit(20))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
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
                + "  from table a "
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

        TableSourceReference tableB = of("es", "tableB", "b");
        ColumnReference bAst = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
        Schema schemaB = Schema.of(CoreColumn.of(bAst, ResolvedType.of(Type.Any)));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        projection(
                            tableScan(schemaB, tableB),
                            asList(e("col1"), e("col2"), e("col3"))),
                    "x"),
                    asList(e("x.col1"), e("x.col2"), e("x.col3 + x.col2")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                        tableScan(schemaB, tableB),
                    asList(cre(bAst.rename("col1")), cre(bAst.rename("col2")), add(cre(bAst.rename("col3")), cre(bAst.rename("col2")))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

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

        TableSourceReference tableB = of("es", "tableB", "b");
        ColumnReference bRef = new ColumnReference(tableB, "b", ColumnReference.Type.REGULAR);
        CoreColumn col1 = col(bRef.rename("col1"), Type.Int);
        CoreColumn col2 = col(bRef.rename("col2"), Type.Float);
        CoreColumn col3 = col(bRef.rename("col3"), Type.String);
        Schema schemaB = Schema.of(col1, col2, col3);

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        projection(
                            tableScan(schemaB, tableB),
                            asList(e("col1"), e("col2"), e("col3"))),
                    "x"),
                    asList(e("x.col1"), e("x.col2"), e("x.col3 + x.col2")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                        tableScan(schemaB, tableB),
                    asList(
                        cre(col1.getColumnReference(), 0, ResolvedType.of(Type.Int)),
                        cre(col2.getColumnReference(), 1, ResolvedType.of(Type.Float)),
                        add(
                            cre(col3.getColumnReference(), 2, ResolvedType.of(Type.String)),
                            cre(col2.getColumnReference(), 1, ResolvedType.of(Type.Float)))));
        //@formatter:on

        Schema expectedSchema = Schema.of(col1, col2, new CoreColumn("", ResolvedType.of(Type.Float), "b.col3 + b.col2", false));
        assertEquals(expectedSchema, actual.getSchema());

        // System.out.println(expectedSchema);
        // System.out.println(actual.getSchema());
        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_nested_subqueries_are_eliminated()
    {
        TableSourceReference table = of("es", "tableB", "b");
        ColumnReference ast = new ColumnReference(table, "b", ColumnReference.Type.ASTERISK);
        Schema schema = Schema.of(col("b", Type.Any, ast));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        subQuery(
                            subQuery(
                                tableScan(schema, table),
                            "x"), 
                        "y"),
                    "z"),
                    asList(e("z.col1"), e("col2")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    tableScan(schema, table),
                    asList(cre(ast.rename("col1")), cre(ast.rename("col2"))));
        //@formatter:on

        Schema expectedSchema = Schema.of(col(ast.rename("col1"), Type.Any), col(ast.rename("col2"), Type.Any));

        // System.out.println(actual.getSchema());
        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expectedSchema, actual.getSchema());
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

        TableSourceReference table = of("es", "tableB", "b");
        ColumnReference bAst = new ColumnReference(table, "b", ColumnReference.Type.ASTERISK);
        Schema schema = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        tableScan(schema, table),
                        "x"),
                    asList(e("x.col1"), e("col2")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                        tableScan(schema, table),
                    asList(cre(bAst.rename("col1")), cre(bAst.rename("col2"))));
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

        TableSourceReference tableB = of("es", "tableB", "b");
        TableSourceReference tableC = of("es", "tableC", "c");

        ColumnReference bCol1 = new ColumnReference(tableB, "col1", ColumnReference.Type.REGULAR);
        ColumnReference bCol2 = new ColumnReference(tableB, "col2", ColumnReference.Type.REGULAR);
        ColumnReference cCol1 = new ColumnReference(tableC, "col1", ColumnReference.Type.REGULAR);
        ColumnReference cCol2 = new ColumnReference(tableC, "col2", ColumnReference.Type.REGULAR);
        ColumnReference cCol3 = new ColumnReference(tableC, "col3", ColumnReference.Type.REGULAR);

        Schema schemaB = Schema.of(col(bCol1, Type.Int), col(bCol2, Type.String));
        Schema schemaC = Schema.of(col(cCol1, Type.Int), col(cCol2, Type.Int), col(cCol3, Type.String));

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
                        false),
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

        TableSourceReference tableB = of("es", "tableB", "b");
        TableSourceReference tableC = of("es", "tableC", "c");

        ColumnReference bCol = new ColumnReference(tableB, "a", ColumnReference.Type.ASTERISK);
        ColumnReference cCol = new ColumnReference(tableC, "c", ColumnReference.Type.ASTERISK);

        Schema schemaB = Schema.of(col(bCol, Type.Any));
        Schema schemaC = Schema.of(col(cCol, Type.Any));

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
                        false),
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
         *   inner join tableC 
         *     c on c.col1 = b.col1 
         * ) x
         * @formatter:on
         */

        TableSourceReference tableB = of("es", "tableB", "b");
        TableSourceReference tableC = of("es", "tableC", "c");

        ColumnReference bCol1 = new ColumnReference(tableB, "col1", ColumnReference.Type.REGULAR);
        ColumnReference bCol2 = new ColumnReference(tableB, "col2", ColumnReference.Type.REGULAR);
        ColumnReference cCol1 = new ColumnReference(tableC, "col1", ColumnReference.Type.REGULAR);
        ColumnReference cCol2 = new ColumnReference(tableC, "col2", ColumnReference.Type.REGULAR);
        ColumnReference cCol3 = new ColumnReference(tableC, "col3", ColumnReference.Type.REGULAR);

        Schema schemaB = Schema.of(col(bCol1, Type.Int), col(bCol2, Type.String));
        Schema schemaC = Schema.of(col(cCol1, Type.Int), col(cCol2, Type.Int), col(cCol3, Type.String));

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
                            false)
                    , "x"),
                    asList(e("x.col1"), e("x.col2")));
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of ambiguous column");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Ambiguous column: x.col1"));
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

        TableSourceReference tableB = of("es", "tableB", "b");
        TableSourceReference tableC = of("es", "tableC", "c");

        ColumnReference bCol1 = new ColumnReference(tableB, "col1", ColumnReference.Type.REGULAR);
        ColumnReference bCol2 = new ColumnReference(tableB, "col2", ColumnReference.Type.REGULAR);
        ColumnReference cCol1 = new ColumnReference(tableC, "col1", ColumnReference.Type.REGULAR);
        ColumnReference cCol2 = new ColumnReference(tableC, "col2", ColumnReference.Type.REGULAR);
        ColumnReference cCol3 = new ColumnReference(tableC, "col3", ColumnReference.Type.REGULAR);

        Schema schemaB = Schema.of(col(bCol1, Type.Int), col(bCol2, Type.String));
        Schema schemaC = Schema.of(col(cCol1, Type.Int), col(cCol2, Type.Int), col(cCol3, Type.String));

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
                            false)
                    , "x"),
                    asList(e("x.col5"), e("x.col2")));
        //@formatter:on

        try
        {
            optimize(context, plan);
            fail("Should fail cause of unknown column");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("x.col5 cannot be bound"));
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

        TableSourceReference tableB = of("es", "tableB", "b");
        ColumnReference col1 = new ColumnReference(tableB, "col1", ColumnReference.Type.REGULAR);
        ColumnReference col2 = new ColumnReference(tableB, "col2", ColumnReference.Type.REGULAR);
        Schema schema = Schema.of(col(col1, Type.Int), col(col2, Type.String));

        //@formatter:off
        ILogicalPlan plan = 
                projection(
                    subQuery(
                        tableScan(schema, tableB), "x"),
                    asList(e("x.col1"), e("col2")));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    tableScan(schema, tableB),
                    asList(cre(col1, 0, ResolvedType.of(Type.Int)), cre(col2, 1, ResolvedType.of(Type.String))));
        //@formatter:on

        // System.out.println(expected.getSchema());
        // System.out.println(actual.getSchema());
        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(expected);

        assertEquals(schema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_filter_with_sub_query_static_schema()
    {
        TableSourceReference tableB = of("es", "tableB", "b");
        ColumnReference col1 = new ColumnReference(tableB, "col1", ColumnReference.Type.REGULAR);
        ColumnReference col2 = new ColumnReference(tableB, "col2", ColumnReference.Type.REGULAR);
        Schema schema = Schema.of(col(col1, Type.Int), col(col2, Type.String));

        //@formatter:off
        ILogicalPlan plan = 
                new Filter(
                    subQuery(
                        tableScan(schema, tableB),
                        "x"),
                    null,
                    e("x.col1 > 10"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    tableScan(schema, tableB),
                    null,
                    gt(cre(col1, 0, ResolvedType.of(Type.Int)), new LiteralIntegerExpression(10)));
                    
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(expected);

        assertEquals(schema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_filter_with_sub_query_schema_less()
    {
        TableSourceReference table = of("es", "tableB", "b");
        ColumnReference bAst = new ColumnReference(table, "b", ColumnReference.Type.ASTERISK);
        Schema schema = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan plan = 
                new Filter(
                    subQuery(
                        tableScan(schema, table), "x"),
                    null,
                    e("x.col1 > 10"));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    tableScan(schema, table),
                    null,
                    gt(cre(bAst.rename("col1")), new LiteralIntegerExpression(10)));
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

        TableSourceReference tableSource = new TableSourceReference("", QualifiedName.of("range"), "x");
        ColumnReference value = tableSource.column("Value");

        Schema expectedSchema = Schema.of(CoreColumn.of("Value", ResolvedType.of(Type.Int), "", false, value));

        //@formatter:off
        ILogicalPlan expected =
                        new TableFunctionScan(tableSource, expectedSchema, asList(intLit(1), intLit(10)), emptyList(), null);
        //@formatter:on

        assertEquals(expectedSchema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_sort_with_sub_query_static_schema()
    {
        TableSourceReference table = of("es", "tableB", "b");
        ColumnReference col1 = new ColumnReference(table, "col1", ColumnReference.Type.REGULAR);
        ColumnReference col2 = new ColumnReference(table, "col2", ColumnReference.Type.REGULAR);
        Schema schema = Schema.of(col(col1, Type.Int), col(col2, Type.String));

        //@formatter:off
        ILogicalPlan plan = 
                new Sort(
                    subQuery(
                        tableScan(schema, table), "x"),
                    asList(new SortItem(e("x.col1"), Order.ASC, NullOrder.FIRST, null)));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    tableScan(schema, table),
                    asList(new SortItem(cre(col1, 0, ResolvedType.of(Type.Int)), Order.ASC, NullOrder.FIRST, null)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(CommonToken.class, Random.class)
                .isEqualTo(expected);

        assertEquals(schema, actual.getSchema());
        assertEquals(expected, actual);
    }

    @Test
    public void test_sort_with_sub_query_non_static_schema()
    {
        TableSourceReference table = of("es", "tableB", "b");
        ColumnReference bAst = new ColumnReference(table, "b", ColumnReference.Type.ASTERISK);
        Schema schema = Schema.of(col("b", Type.Any, bAst));

        //@formatter:off
        ILogicalPlan plan = 
                new Sort(
                    subQuery(
                        tableScan(schema, table),
                        "x"),
                    asList(new SortItem(e("x.col1"), Order.ASC, NullOrder.FIRST, null)));
        //@formatter:on

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    tableScan(schema, table),
                    asList(new SortItem(cre(bAst.rename("col1")), Order.ASC, NullOrder.FIRST, null)));
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

    public static TableSourceReference of(String catalogAlias, String name, String alias)
    {
        return new TableSourceReference(catalogAlias, QualifiedName.of(name), alias);
    }
}
