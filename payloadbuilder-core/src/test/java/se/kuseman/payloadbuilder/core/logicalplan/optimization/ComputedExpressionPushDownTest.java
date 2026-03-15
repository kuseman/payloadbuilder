package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static se.kuseman.payloadbuilder.api.QualifiedName.of;

import java.util.List;
import java.util.Random;

import org.assertj.core.api.Assertions;
import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.LambdaExpression;
import se.kuseman.payloadbuilder.core.expression.SubscriptExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedSubQueryExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.parser.ParseException;

/** Test of {@link ComputedExpressionPushDown} */
class ComputedExpressionPushDownTest extends ALogicalPlanOptimizerTest
{
    private final ComputedExpressionPushDown optimizer = new ComputedExpressionPushDown();
    private final TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", of("table"), "t");
    private final TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", of("tableB"), "b");

    @Test
    void test_order_by_column_not_projected()
    {
        // SQL standard specifies that an order by column reference points either to
        // a projected column or a table source. Normally the col2 is not present in the projections input
        // so we need to adapt for that

        //@formatter:off
        String query = """
                select b.col1
                from "table" t
                inner join "tableB" b
                  on b.col = t.col
                order by t.col1
                """;
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        //CSOFF
        ILogicalPlan expected = new Sort(
                new Projection(
                        new Join(
                            tableScan(Schema.of(ast("t", table)), table),
                            tableScan(Schema.of(ast("b", tableB)), tableB),
                            Join.Type.INNER,
                            null,
                            e("b.col = t.col"),
                            emptySet(),
                            false,
                            Schema.EMPTY),
                        List.of(e("b.col1"), new AliasExpression(e("t.col1"), "col1", true)),
                        null),
                List.of(sortItem(e("t.col1"), Order.ASC))
                );
        //CSON
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
    void test_order_by_and_projection_with_subscript()
    {
        //@formatter:off
        String query = """
                select concat(col, 1)[0]
                from "table" t
                order by concat(col, 1)[0]
                """;
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        //CSOFF
        ILogicalPlan expected = new Sort(
                    new Projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        List.of(new AliasExpression(new SubscriptExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("concat"), null,
                                asList(uce("col"), intLit(1))), intLit(0)), "__expr0", "concat(col, 1)[0]", false)),
                        null),
                List.of(sortItem(uce("__expr0"), Order.ASC))
                );
        //CSON
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
    void test_sort_expression_with_group_by()
    {
        //@formatter:off
        // Here we have a complex order by expression which contains columns from the aggregate
        // but they are not projected and we want these 2 columns to be added to projection as internal expressions
        String query = "select col1, max(col2) "
                + "from \"table\" t "
                + "group by col1, col3 "
                + "order by col1 + col3 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = new Sort(
                new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col1"), e("col3")),
                        asList(
                            new AggregateWrapperExpression(e("col1"), false, false),
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))),
                            new AggregateWrapperExpression(e("col3"), false, true)
                        ),
                        null),
                asList(sortItem(add(e("col1"), e("col3")), Order.ASC))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_sort_on_non_aggregated_column_contained_in_aggregate_function_verify_push_down()
    {
        //@formatter:off
        String query = "select col1, max(col2) "
                + "from \"table\" t "
                + "group by col1, col3 "
                + "order by min(col1 + col2) ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = new Sort(
                new Aggregate(
                        projection(
                            tableScan(Schema.of(ast("t", table)), table),
                            asList(new AliasExpression(add(e("col1"), e("col2")), "__expr0"),
                                    new AsteriskExpression(null))),
                        asList(e("col1"), e("col3")),
                        asList(
                            new AggregateWrapperExpression(e("col1"), false, false),
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))),
                            new AggregateWrapperExpression(new AliasExpression(
                                    new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("min"), null, asList(uce("__expr0"))), "__expr1"), false, true)
                        ),
                        null),
                asList(sortItem(uce("__expr1"), Order.ASC))
                );
        
        assertEquals(Schema.of(
                col("col1", ResolvedType.array(Type.Any)),
                col(ResolvedType.of(Type.Any), "max(col2)"),
                col("__expr1", ResolvedType.of(Type.Any), true)        // Internal
                ), actual.getSchema());
        
        //@formatter:on

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_sort_on_non_aggregated_column_contained_in_aggregate_function_verify_push_down_in_sub_query_expression()
    {
        //@formatter:off
        String query = "select ( "
                + "select col1, max(col2) "
                + "from \"table\" t "
                + "group by col1, col3 "
                + "order by min(col1 + col2) "
                + ") x ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                ConstantScan.create(
                        asList(new AliasExpression(new UnresolvedSubQueryExpression(
                            new Sort(
                                new Aggregate(
                                    projection(
                                        tableScan(Schema.of(ast("t", table)), table),
                                        asList(new AliasExpression(add(e("col1"), e("col2")), "__expr0"), new AsteriskExpression(null))),
                                    asList(e("col1"), e("col3")),
                                    asList(
                                        new AggregateWrapperExpression(e("col1"), false, false),
                                        new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))),
                                        new AggregateWrapperExpression(new AliasExpression(
                                                new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("min"), null, asList(uce("__expr0"))), "__expr1"), false, true)
                                    ),
                                    null),
                            asList(sortItem(uce("__expr1"), Order.ASC))),
                            null), "x")), null);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(Schema.of(col("x", ResolvedType.array(Type.Any))), actual.getSchema());

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    void test_double_nested_sub_query_expressions_with_order_bys()
    {
        //@formatter:off
        String query = "select "
                + "( "
                + "   select "
                + "   ( "
                + "     select b.col1 "
                + "     from stableB b "
                + "     order by b.col2 "
                + "     for object "
                + "   ) obj1"                           // Verify that this second nested sub query gets processed
                + "   from stableA a "
                + "   order by a.col2 "
                + "   for object_array "
                + ") \"values\" ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        Schema resolvedSchemaSTableA = Schema.of(
                col("col1", Type.Int, sTableA),
                col("col2", Type.String, sTableA),
                col("col3", Type.Float, sTableA));
        Schema resolvedSchemaSTableB = Schema.of(
                col("col1", Type.Boolean, sTableB),
                col("col2", Type.String, sTableB),
                col("col3", Type.Float, sTableB));
        //@formatter:on

        //@formatter:off
        ILogicalPlan expected =
                ConstantScan.create(
                        asList(new AliasExpression(new UnresolvedSubQueryExpression(
                                new OperatorFunctionScan(
                                    Schema.of(Column.of("output", Type.Any)),
                                    new Sort(
                                        new Projection(
                                            tableScan(resolvedSchemaSTableA, sTableA),
                                            asList(new AliasExpression(new UnresolvedSubQueryExpression(
                                                new OperatorFunctionScan(
                                                    Schema.of(Column.of("output", Type.Any)),
                                                    new Sort(
                                                        new Projection(
                                                            tableScan(resolvedSchemaSTableB, sTableB),
                                                            asList(uce("b", "col1"),
                                                            new AliasExpression(uce("b", "col2"), "col2", true)),            // Internal column added for sorting
                                                            null),
                                                        asList(sortItem(uce("b", "col2"), Order.ASC))),
                                                    "",
                                                    "object",
                                                    null),
                                                null),
                                                "obj1"),
                                                new AliasExpression(uce("a", "col2"), "col2", true)),                       // Internal column added for sorting
                                            null),
                                        asList(sortItem(uce("a", "col2"), Order.ASC))),
                                    "",
                                    "object_array",
                                    null),
                                null
                                ),
                            "values")), null);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(Schema.of(col("values", ResolvedType.of(Type.Any))), actual.getSchema());

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    void test_sort_on_aggregate_function_that_is_also_projected()
    {
        //@formatter:off
        // Here we should change the aggregate projection to the pushed down alias only
        // we are projecting the same expression as we sort on
        String query = "select col1, max(col2), min(COL1 + col2) "
                + "from \"table\" t "
                + "group by col1, col3 "
                + "order by min(col1 + col2) ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = new Sort(
                new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col1"), e("col3")),
                        asList(
                            new AggregateWrapperExpression(e("col1"), false, false),
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))),
                            new AggregateWrapperExpression(new AliasExpression(
                                    new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("min"), null,
                                            asList(add(e("COL1"), e("col2")))), "__expr0", "min(COL1 + col2)", false), false, false)
                        ),
                        null),
                asList(sortItem(uce("__expr0"), Order.ASC))
                );
        
        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(Schema.of(
                col("col1", ResolvedType.array(Type.Any)),
                col(ResolvedType.of(Type.Any), "max(col2)"),
                col("__expr0", ResolvedType.of(Type.Any), "min(COL1 + col2)")
                ));
        //@formatter:on
        //
        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    void test_sort_on_aggregate_function_that_is_also_projected_and_aliased()
    {
        //@formatter:off
        // Here we should change the aggregate projection to the pushed down alias only
        // we are projecting the same expression as we sort on
        String query = "select col1, max(col2), min(col1 + col2) compute "
                + "from \"table\" t "
                + "group by col1, col3 "
                + "order by min(col1 + col2) ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = new Sort(
                new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col1"), e("col3")),
                        asList(
                            new AggregateWrapperExpression(e("col1"), false, false),
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))),
                            new AggregateWrapperExpression(new AliasExpression(
                                    new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("min"), null,
                                            asList(add(e("col1"), e("col2")))), "compute", false), false, false)
                        ),
                        null),
                asList(sortItem(e("compute"), Order.ASC))
                );
        
        assertEquals(Schema.of(
                col("col1", ResolvedType.array(Type.Any)),
                col(ResolvedType.of(Type.Any), "max(col2)"),
                col("compute", ResolvedType.of(Type.Any))
                ), actual.getSchema());
        
        //@formatter:on
        //
        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_sort_on_non_aggregated_column()
    {
        //@formatter:off
        String query = "select col1, max(col2) "
                + "from \"table\" t "
                + "group by col1, col3 "
                + "order by  col1 + col2";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = new Sort(
                new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col1"), e("col3")),
                        asList(
                            new AggregateWrapperExpression(e("col1"), false, false),
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))),
                            new AggregateWrapperExpression(e("col2"), false, true)
                        ),
                        null),
                asList(sortItem(e("col1 + col2"), Order.ASC))
                );

        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .isEqualTo(Schema.of(
                col("col1", ResolvedType.array(Type.Any)),
                col(ResolvedType.of(Type.Any), "max(col2)"),
                col("col2", ResolvedType.array(Type.Any), true)
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
    void test_sort_by_ordinal_out_of_range()
    {
        //@formatter:off
        String query = "select col1, col2 "
                + "from \"table\" t "
                + "order by 3 ";
        //@formatter:on

        try
        {
            ILogicalPlan plan = getSchemaResolvedPlan(query);
            optimize(context, plan);
            fail("Should fail with out of range");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage()
                    .contains("ORDER BY position is out of range"), e.getMessage());
        }
    }

    @Test
    void test_sort_by_ordinal_out_of_range_1()
    {
        //@formatter:off
        String query = "select col1, col2 "
                + "from \"table\" t "
                + "order by 0 ";
        //@formatter:on

        try
        {
            ILogicalPlan plan = getSchemaResolvedPlan(query);
            optimize(context, plan);
            fail("Should fail with out of range");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage()
                    .contains("ORDER BY position is out of range"), e.getMessage());
        }
    }

    @Test
    void test_sort_by_ordinal_out_of_range_2()
    {
        //@formatter:off
        String query = "select *, col1, col2 "
                + "from \"table\" t "
                + "order by 0 ";
        //@formatter:on

        try
        {
            ILogicalPlan plan = getSchemaResolvedPlan(query);
            optimize(context, plan);
            fail("Should fail with out of range");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage()
                    .contains("ORDER BY position is out of range"), e.getMessage());
        }
    }

    @Test
    void test_sort_by_ordinal_with_asterisk()
    {
        //@formatter:off
        String query = "select * "
                + "from \"table\" t "
                + "order by 1 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        // Will be any push downs here since we have an asterisk and will have to resolve ordinal runtime
        //@formatter:off
        ILogicalPlan expected = new Sort(
                projection(
                    tableScan(Schema.of(ast("t", table)), table),
                    List.of(new AsteriskExpression(null))),
                asList(sortItem(intLit(1), Order.ASC))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_sort_by_ordinal_with_asterisk_with_sub_query()
    {
        //@formatter:off
        String query = "select * "
                + "from "
                + "( "
                + "  select * "
                + "  from \"table\" t "
                + "  order by 1 "
                + ") x ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference table = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", of("table"), "t");
        TableSourceReference subQueryX = new TableSourceReference(0, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");

        // Will be any push downs here since we have an asterisk and will have to resolve ordinal runtime
        //@formatter:off
        ILogicalPlan expected =
                projection(
                    subQuery(
                        new Sort(
                            projection(
                                tableScan(Schema.of(ast("t", table)), table),
                                List.of(new AsteriskExpression(null))),
                            asList(sortItem(intLit(1), Order.ASC))),
                        subQueryX),
                    List.of(new AsteriskExpression(null)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_sort_by_ordinal_with_non_asterisk()
    {
        //@formatter:off
        String query = "select col "
                + "from \"table\" t "
                + "order by 1 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        // Expression on ordinal 1 will be used
        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col"))),
                    asList(sortItem(new UnresolvedColumnExpression(QualifiedName.of("col"), -1, null), Order.ASC)));
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
    void test_sort_by_ordinal_with_aliased_column()
    {
        //@formatter:off
        String query = "select col fancyColumn "
                + "from \"table\" t "
                + "order by 1 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        // Expression on ordinal 1 will be used
        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AliasExpression(e("col"), "fancyColumn"))),
                    asList(sortItem(new UnresolvedColumnExpression(QualifiedName.of("fancyColumn"), -1, null), Order.ASC)));
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
    void test_sort_by_ordinal_with_non_asterisk_with_subquery()
    {
        //@formatter:off
        String query = "select * "
                + "from "
                + "( "
                + "  select col "
                + "  from \"table\" t "
                +   "order by 1 "
                + ") x ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        TableSourceReference table = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", of("table"), "t");
        TableSourceReference subQueryX = new TableSourceReference(0, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");

        // Expression on ordinal 1 will be used
        //@formatter:off
        ILogicalPlan expected =
                projection(
                    subQuery(
                        new Sort(
                            projection(
                                tableScan(Schema.of(ast("t", table)), table),
                                asList(e("col"))),
                            asList(sortItem(new UnresolvedColumnExpression(QualifiedName.of("col"), -1, null), Order.ASC))),
                        subQueryX),
                    List.of(new AsteriskExpression(null)));
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
    void test_sort_by_ordinal_on_computed_expression_with_non_asterisk()
    {
        //@formatter:off
        String query = "select col + 10 "
                + "from \"table\" t "
                + "order by 1 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AliasExpression(add(e("col"), intLit(10)), "__expr0", "col + 10", false))),
                    asList(sortItem(new UnresolvedColumnExpression(QualifiedName.of("__expr0"), -1, null), Order.ASC)));
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
    void test_sort_by_ordinal_on_computed_alias_expression_with_non_asterisk()
    {
        //@formatter:off
        String query = "select col + 10 calc "
                + "from \"table\" t "
                + "order by col + 10 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AliasExpression(add(e("col"), intLit(10)), "calc", false))),
                    asList(sortItem(new UnresolvedColumnExpression(QualifiedName.of("calc"), -1, null), Order.ASC)));
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
    void test_sort_by_ordinal_on_computed_expression_with_asterisk()
    {
        //@formatter:off
        String query = "select col + 10, * "
                + "from \"table\" t "
                + "order by 1 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AliasExpression(add(e("col"), intLit(10)), "__expr0", "col + 10", false), new AsteriskExpression(null))),
                    asList(sortItem(new UnresolvedColumnExpression(QualifiedName.of("__expr0"), -1, null), Order.ASC)));
        
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
    void test_sort_by_ordinal_with_mixed_asterisk_last()
    {
        //@formatter:off
        String query = "select col, *"
                + "from \"table\" t "
                + "order by 1 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col"), new AsteriskExpression(null))),
                    asList(sortItem(new UnresolvedColumnExpression(QualifiedName.of("col"), -1, null), Order.ASC)));
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
    void test_sort_by_ordinal_with_mixed_asterisk_first()
    {
        //@formatter:off
        String query = "select *, col "
                + "from \"table\" t "
                + "order by 1 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        // Cannot rewrite since we have an asterisk before or on ordinal
        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AsteriskExpression(null), e("col"))),
                    asList(sortItem(intLit(1), Order.ASC)));
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
    void test_sort_by_ordinal_with_mixed_asterisk_first_2()
    {
        //@formatter:off
        String query = "select *, col1, col "
                + "from \"table\" t "
                + "order by 2 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        // Cannot rewrite since we have an asterisk before or on ordinal
        //@formatter:off
        ILogicalPlan expected = 
                    new Sort(
                            projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AsteriskExpression(null), e("col1"), e("col"))),
                    asList(sortItem(intLit(2), Order.ASC)));
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
    void test_sort_by_qualifier_asterisk()
    {
        //@formatter:off
        String query = "select * "
                + "from \"table\" t "
                + "order by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                    new Sort(
                        projection(
                            tableScan(Schema.of(ast("t", table)), table),
                            List.of(new AsteriskExpression(null))),
                        asList(sortItem(e("col"), Order.ASC)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_sort_by_computed_expression_with_no_alias()
    {
        //@formatter:off
        String query = "select col1 + col2 "
                + "from \"table\" t "
                + "order by col2 + col1";               // Semantic equals artihmetics, operands are switched
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AliasExpression(add(e("col1"), e("col2")), "__expr0", "col1 + col2", false))),
                    asList(sortItem(new UnresolvedColumnExpression(QualifiedName.of("__expr0"), -1, null), Order.ASC)));
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
    void test_sort_by_qualifier_non_asterisk()
    {
        //@formatter:off
        String query = "select col "
                + "from \"table\" t "
                + "order by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col"))),
                    asList(sortItem(uce("col"), Order.ASC)));
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
    void test_sort_by_non_projected_column()
    {
        //@formatter:off
        String query = "select col "
                + "from \"table\" t "
                + "order by col2 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(
                                e("col"),
                                new AliasExpression(new UnresolvedColumnExpression(QualifiedName.of("col2"), -1, null), "col2", true)
                                )),
                    asList(sortItem(e("col2"), Order.ASC)));
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
    void test_sort_by_alias_qualifier_non_asterisk()
    {
        //@formatter:off
        String query = "select col newCol "
                + "from \"table\" t "
                + "order by newCol ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AliasExpression(e("col"), "newCol"))),
                    asList(sortItem(e("newCol"), Order.ASC)));
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
    void test_sort_by_alias_qualifier_non_asterisk_2()
    {
        //@formatter:off
        String query = "select col newCol "
                + "from \"table\" t "
                + "order by t.newCol ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        // Sort item is a aliased expression and hence it's not the projected column that is assumed
        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AliasExpression(e("col"), "newCol"))),
                    asList(sortItem(e("t.newCol"), Order.ASC)));
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
    void test_sort_by_alias_qualifier_asterisk()
    {
        //@formatter:off
        String query = "select col newCol, * "
                + "from \"table\" t "
                + "order by newCol ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AliasExpression(e("col"), "newCol"), new AsteriskExpression(null))),
                    asList(sortItem(e("newCol"), Order.ASC)));
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
    void test_sort_by_alias_qualifier_asterisk_before()
    {
        //@formatter:off
        String query = "select *, col newCol "
                + "from \"table\" t "
                + "order by newCol ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AsteriskExpression(null), new AliasExpression(e("col"), "newCol"))),
                    asList(sortItem(e("newCol"), Order.ASC)));
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
    void test_sort_by_alias_qualifier_with_computed_expression()
    {
        //@formatter:off
        String query = "select col + 10 newCol "
                + "from \"table\" t "
                + "order by newCol ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    projection(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(new AliasExpression(e("col + 10"), "newCol"))),
                    asList(sortItem(e("newCol"), Order.ASC)));
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
    void test_aggregate()
    {
        //@formatter:off
        // Mixed expression: count(1) is a pure aggregate, but t.col + max(col1) contains an aggregate function
        // wrapped in a non-aggregate operation. According to ANSI SQL the aggregate functions should be pushed
        // into the Aggregate operator and the outer computation placed in a Projection above.
        String query = "select count(1), t.col + max(col1) "
                + "from \"table\" t "
                + "group by t.col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("t.col")),
                        asList(
                            // count(1) gets a generated alias so the outer Projection can reference it
                            new AggregateWrapperExpression(
                                    new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "__expr0", "count(1)", false),
                                    false, false),
                            // max(col1) extracted from the mixed expression as an internal projection
                            new AggregateWrapperExpression(
                                    new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col1"))), "__expr1"),
                                    false, true),
                            // t.col is a non-aggregated reference from the mixed expression, added as internal
                            new AggregateWrapperExpression(e("t.col"), false, true)
                        ),
                        null),
                    // Outer projection: pass count(1) through and compute the mixed expression
                    asList(uce("__expr0"), add(e("t.col"), uce("__expr1"))),
                    null);

        assertEquals(Schema.of(
                col("__expr0", ResolvedType.of(Type.Any)),
                col(ResolvedType.of(Type.Any), "t.col + __expr1")),
                actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_with_non_aggregate_expression_on_aggregate_function()
    {
        //@formatter:off
        // Mixed expressions: both max(col1) + 1 and count(*) * 2 wrap an aggregate function
        // in a non-aggregate operation and should be split.
        String query = "select max(col1) + 1, count(1) * 2 "
                + "from \"table\" t "
                + "group by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col")),
                        asList(
                            // max(col1) extracted from first mixed expression
                            new AggregateWrapperExpression(
                                    new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col1"))), "__expr0"),
                                    false, true),
                            // count(1) extracted from second mixed expression
                            new AggregateWrapperExpression(
                                    new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "__expr1"),
                                    false, true)
                        ),
                        null),
                    asList(add(uce("__expr0"), intLit(1)), e("__expr1 * 2")),
                    null);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_with_mixed_expression_and_having()
    {
        //@formatter:off
        // Mixed expression in SELECT combined with a HAVING clause.
        // The aggregate function in the mixed SELECT expression and the HAVING aggregate function
        // should both be extracted as internal Aggregate projections.
        String query = "select col, max(col1) + 1 myCol "
                + "from \"table\" t "
                + "group by col "
                + "having count(1) > 10 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        // When mixed expressions are present and there is a HAVING clause,
        // the Filter is placed between the Aggregate and the outer Projection
        // so it can reference internal aggregate aliases (__exprN).
        ILogicalPlan expected =
                new Projection(
                    new Filter(
                        new Aggregate(
                            tableScan(Schema.of(ast("t", table)), table),
                            asList(e("col")),
                            asList(
                                // col is a group-by reference, stays in Aggregate as pass-through
                                new AggregateWrapperExpression(e("col"), false, false),
                                // max(col1) extracted from the mixed expression
                                new AggregateWrapperExpression(
                                        new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col1"))), "__expr0"),
                                        false, true),
                                // count(1) for HAVING extracted as internal
                                new AggregateWrapperExpression(
                                        new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "__expr1"),
                                        false, true)
                            ),
                            null),
                        null,
                        e("__expr1 > 10")),
                    // Outer projection: pass col through and compute the mixed expression with alias myCol
                    asList(uce("col"), new AliasExpression(add(uce("__expr0"), intLit(1)), "myCol")),
                    null);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_with_non_aggregate_function_wrapping_aggregate()
    {
        //@formatter:off
        // A non-aggregate function (concat) wrapping an aggregate (max) should be
        // split: max(col2) stays in the Aggregate as an internal projection,
        // and concat('prefix', __expr0) goes in the outer Projection.
        String query = "select col, concat('prefix', max(col2)) max_col "
                + "from \"table\" t "
                + "group by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col")),
                        asList(
                            new AggregateWrapperExpression(e("col"), false, false),
                            new AggregateWrapperExpression(
                                    new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))), "__expr0"),
                                    false, true)
                        ),
                        null),
                    asList(uce("col"), new AliasExpression(
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("concat"), null, asList(e("'prefix'"), uce("__expr0"))),
                            "max_col")),
                    null);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_aliased_column()
    {
        //@formatter:off
        String query = "select count(1) cnt, col "
                + "from \"table\" t "
                + "group by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Aggregate(
                    tableScan(Schema.of(ast("t", table)), table),
                    asList(e("col")),
                    asList(new AggregateWrapperExpression(
                                new AliasExpression(
                                    new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "cnt"), false, false),
                           new AggregateWrapperExpression(e("col"), false, false)),
                    null);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_with_having()
    {
        //@formatter:off
        String query = "select count(1) "
                + "from \"table\" t "
                + "group by col "
                + "having max(col2) > 10 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col")),
                        asList(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))),
                               new AggregateWrapperExpression(
                                       new AliasExpression(
                                           new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))), "__expr0"), false, true)
                                ),
                        null),
                    null,
                    e("__expr0 > 10"));

        assertEquals(Schema.of(
                col(ResolvedType.of(Type.Int), "count(1)"),
                col("__expr0", ResolvedType.of(Type.Any), true)),
                actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_with_having_and_where_make_sure_they_dont_interfere()
    {
        //@formatter:off
        String query = "select count(1) "
                + "from \"table\" t "
                + "where col1 > 10 "
                + "group by col "
                + "having max(col2) > 10 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Aggregate(
                        new Filter(
                            tableScan(Schema.of(ast("t", table)), table),
                            null,
                            e("col1 > 10")),
                        asList(e("col")),
                        asList(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))),
                               new AggregateWrapperExpression(
                                       new AliasExpression(
                                           new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))), "__expr0"), false, true)
                                ),
                        null),
                    null,
                    e("__expr0 > 10"));

        assertEquals(Schema.of(
                col(ResolvedType.of(Type.Int), "count(1)"),
                col("__expr0", ResolvedType.of(Type.Any), true)),
                actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_with_having_and_order_by_complex_expression()
    {
        //@formatter:off
        String query = "select count(1) "
                + "from \"table\" t "
                + "group by col "
                + "having max(col2) < 10 and (2 + min(col3 * 2)) > 20 "
                + "order by min(2 * col3) ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    new Filter(
                        new Aggregate(
                            projection(
                                tableScan(Schema.of(ast("t", table)), table),
                                asList(new AliasExpression(e("2 * col3"), "__expr0"), new AsteriskExpression(null))),
                            asList(e("col")),
                            asList(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))),
                                    new AggregateWrapperExpression(
                                            new AliasExpression(
                                               new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("min"), null, asList(e("__expr0"))), "__expr1"), false, true),
                                   new AggregateWrapperExpression(
                                           new AliasExpression(
                                               new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))), "__expr2"), false, true)
                                    ),
                            null),
                        null,
                        e("__expr2 < 10 and (2 + __expr1) > 20")),
                    asList(sortItem(e("__expr1"), Order.ASC)));

        assertEquals(Schema.of(
                col(ResolvedType.of(Type.Int), "count(1)"),
                col("__expr1", ResolvedType.of(Type.Any), true),
                col("__expr2", ResolvedType.of(Type.Any), true)),
                actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        // Assertions.assertThat(actual)
        // .usingRecursiveComparison()
        // .ignoringFieldsOfTypes(Location.class, Random.class)
        // .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_with_having_and_order_by_complex_expression_in_projection()
    {
        //@formatter:off
        String query = "select count(1), min(col3 * 2) "
                + "from \"table\" t "
                + "group by col "
                + "having max(col2) < 10 and (2 + min(col3 * 2)) > 20 "
                + "order by min(2 * col3) ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    new Filter(
                        new Aggregate(
                            tableScan(Schema.of(ast("t", table)), table),
                            asList(e("col")),
                            asList(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))),
                                    new AggregateWrapperExpression(
                                            new AliasExpression(
                                               new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("min"), null, asList(e("col3 * 2"))), "__expr0", "min(col3 * 2)", false),
                                            false, false),
                                   new AggregateWrapperExpression(
                                           new AliasExpression(
                                               new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))), "__expr1"), false, true)
                                    ),
                            null),
                        null,
                        e("__expr1 < 10 and (2 + __expr0) > 20")),
                    asList(sortItem(e("__expr0"), Order.ASC)));

        assertEquals(Schema.of(
                col(ResolvedType.of(Type.Int), "count(1)"),
                col("__expr0", ResolvedType.of(Type.Int), "min(col3 * 2)"),
                col("__expr1", ResolvedType.of(Type.Any), true)),
                actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_with_having_with_same_projection_expression()
    {
        //@formatter:off
        String query = "select count(1) "
                + "from \"table\" t "
                + "group by col "
                + "having count(1) > 10 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Filter(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col")),
                        asList(
                               new AggregateWrapperExpression(
                                       new AliasExpression(
                                           new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "__expr0", "count(1)", false), false, false)
                                ),
                        null),
                    null,
                    e("__expr0 > 10"));

        assertEquals(Schema.of(
                col("__expr0", ResolvedType.of(Type.Int), "count(1)")),
                actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_with_having_and_sort_with_same_projection_expression_unaliased()
    {
        //@formatter:off
        String query = "select count(1) "
                + "from \"table\" t "
                + "group by col "
                + "having count(1) > 10 "
                + "order by count(1) desc";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    new Filter(
                        new Aggregate(
                            tableScan(Schema.of(ast("t", table)), table),
                            asList(e("col")),
                            asList(
                                   new AggregateWrapperExpression(
                                           new AliasExpression(
                                               new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "__expr0", "count(1)", false), false, false)
                                    ),
                            null),
                        null,
                        e("__expr0 > 10")),
                    asList(sortItem(e("__expr0"), Order.DESC)));
        
        assertEquals(Schema.of(
                col("__expr0", ResolvedType.of(Type.Int), "count(1)")),
                actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_with_having_and_sort_with_same_projection_expression_aliased()
    {
        //@formatter:off
        String query = "select count(1) numberOfRecords "
                + "from \"table\" t "
                + "group by col "
                + "having count(1) > 10 "
                + "order by count(1) desc";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    new Filter(
                        new Aggregate(
                            tableScan(Schema.of(ast("t", table)), table),
                            asList(e("col")),
                            asList(
                                   new AggregateWrapperExpression(
                                           new AliasExpression(
                                               new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "numberOfRecords", false), false, false)
                                    ),
                            null),
                        null,
                        e("numberOfRecords > 10")),
                    asList(sortItem(e("numberOfRecords"), Order.DESC)));
        
        assertEquals(Schema.of(
                col("numberOfRecords", ResolvedType.of(Type.Int))),
                actual.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_sort_on_aggregate_aliased_projection_and_verify_pushed_down()
    {
        //@formatter:off
        String query = "select count(1) cnt, col "
                + "from \"table\" t "
                + "group by col "
                + "order by cnt ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col")),
                        asList(new AggregateWrapperExpression(
                                    new AliasExpression(
                                        new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "cnt"), false, false),
                               new AggregateWrapperExpression(e("col"), false, false)
                                ),
                        null),
                    asList(sortItem(e("cnt"), Order.ASC)));

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(Schema.of(
                        col("cnt", ResolvedType.of(Type.Int)),
                        col("col", ResolvedType.array(Type.Any))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_sort_on_aggregate_column_that_is_not_projected_and_verify_pushed_down()
    {
        //@formatter:off
        String query = "select count(1) cnt "
                + "from \"table\" t "
                + "group by t.col "             // qualified
                + "order by col ";              // non qualified but points at the same column
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected = 
                new Sort(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("t.col")),
                        asList(new AggregateWrapperExpression(
                                    new AliasExpression(
                                        new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "cnt"), false, false),
                               new AggregateWrapperExpression(e("col"), false, true)),
                        null),
                    asList(sortItem(e("col"), Order.ASC)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);
    }

    @Test
    void test_sort_on_aggregate_column_with_compute_that_is_not_projected_and_verify_pushed_down()
    {
        //@formatter:off
        String query = "select count(1) cnt "
                + "from \"table\" t "
                + "group by col, col2 "
                + "order by col + 10 + col2 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Sort(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col"), e("col2")),
                        asList(new AggregateWrapperExpression(
                                    new AliasExpression(
                                        new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "cnt"), false, false),
                               new AggregateWrapperExpression(e("col"), false, true),
                               new AggregateWrapperExpression(e("col2"), false, true)),
                        null),
                    asList(sortItem(add(add(e("col"), intLit(10)), e("col2")), Order.ASC)));
        //@formatter:on

        //@formatter:off
        assertEquals(Schema.of(
                col("cnt", ResolvedType.of(Type.Int)),
                col("col", ResolvedType.array(Type.Any), true),      // col internal
                col("col2", ResolvedType.array(Type.Any), true)),     // col2 internsl
                expected.getSchema());
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_error_when_sorting_on_an_aliased_constant()
    {
        //@formatter:off
        String query = "select 'hello' newCol "
                + "from \"table\" t "
                + "order by newCol ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        try
        {
            optimize(context, plan);
            fail("Should fail on order by constant");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage()
                    .contains("ORDER BY constant encountered"), e.getMessage());
        }
    }

    @Test
    void test_error_when_sorting_on_a_constant()
    {
        //@formatter:off
        String query = "select 'hello' "
                + "from \"table\" t "
                + "order by 1 ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);

        try
        {
            optimize(context, plan);
            fail("Should fail on order by constant");
        }
        catch (ParseException e)
        {
            assertTrue(e.getMessage()
                    .contains("ORDER BY constant encountered"), e.getMessage());
        }
    }

    @Test
    void test_aggregate_non_mixed_after_mixed_with_alias()
    {
        //@formatter:off
        // count(1) cnt is non-mixed and appears after the mixed concat(...) expression.
        // Covers the branch: "// Non-mixed: if we already have outer projections, add a pass-through reference"
        String query = "select col, concat('prefix', max(col2)) max_col, count(1) cnt "
                + "from \"table\" t "
                + "group by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col")),
                        asList(
                            new AggregateWrapperExpression(e("col"), false, false),
                            new AggregateWrapperExpression(
                                    new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "cnt"),
                                    false, false),
                            new AggregateWrapperExpression(
                                    new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))), "__expr0"),
                                    false, true)
                        ),
                        null),
                    asList(
                        uce("col"),
                        new AliasExpression(
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("concat"), null, asList(e("'prefix'"), uce("__expr0"))),
                            "max_col"),
                        uce("cnt")),
                    null);
        //@formatter:on

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_non_mixed_after_mixed_without_alias()
    {
        //@formatter:off
        // count(1) without alias is non-mixed and appears after the mixed concat(...) expression.
        // FunctionCallExpression does not implement HasAlias, so no usable alias exists and one
        // must be generated. Covers the branch: "// No usable alias: generate one and update the projection"
        String query = "select col, concat('prefix', max(col2)) max_col, count(1) "
                + "from \"table\" t "
                + "group by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col")),
                        asList(
                            new AggregateWrapperExpression(e("col"), false, false),
                            new AggregateWrapperExpression(
                                    new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("count"), null, asList(intLit(1))), "__expr1", "count(1)", false),
                                    false, false),
                            new AggregateWrapperExpression(
                                    new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))), "__expr0"),
                                    false, true)
                        ),
                        null),
                    asList(
                        uce("col"),
                        new AliasExpression(
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("concat"), null, asList(e("'prefix'"), uce("__expr0"))),
                            "max_col"),
                        uce("__expr1")),
                    null);
        //@formatter:on

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_mixed_without_alias()
    {
        //@formatter:off
        // upper(max(col2)) without an alias is a mixed expression (non-aggregate wrapping an aggregate).
        // Without a fix, the parser returns a bare FunctionCallExpression (since it implements IAggregateExpression)
        // and isMixedProjection() returns false immediately. SchemaResolver now wraps it so CEPD can split it.
        String query = "select col, upper(max(col2)) "
                + "from \"table\" t "
                + "group by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col")),
                        asList(
                            new AggregateWrapperExpression(e("col"), false, false),
                            new AggregateWrapperExpression(
                                    new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col2"))), "__expr0"),
                                    false, true)
                        ),
                        null),
                    asList(
                        uce("col"),
                        new AliasExpression(
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("upper"), null, asList(uce("__expr0"))),
                            "upper(max(col2))")),
                    null);
        //@formatter:on

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_lambda_without_alias_alongside_mixed_expression()
    {
        //@formatter:off
        // col2.map(x -> concat(x, 'ggg')) is a non-mixed AWE whose inner FunctionCallExpression
        // does not implement HasAlias. When upper(max(col3)) forces creation of an outer Projection,
        // getAggregateProjectionRef falls through to the "No usable alias" path: it generates
        // __expr0 and rewrites planProjections[1] to AWE(AliasExpression(map_call, "__expr0", ...)).
        String query = "select col, col2.map(x -> concat(x, 'ggg')), upper(max(col3)) "
                + "from \"table\" t "
                + "group by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        FunctionCallExpression mapCall = new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("map"), null,
                asList(
                    e("col2"),
                    new LambdaExpression(asList("x"),
                        new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("concat"), null,
                            asList(new UnresolvedColumnExpression(of("x"), 0, null), stringLit("ggg"))),
                        new int[] { 0 })
                ));

        ILogicalPlan expected =
                new Projection(
                    new Aggregate(
                        tableScan(Schema.of(ast("t", table)), table),
                        asList(e("col")),
                        asList(
                            new AggregateWrapperExpression(e("col"), false, false),
                            // "No usable alias" path: map call gets a generated alias so the outer Projection can reference it
                            new AggregateWrapperExpression(
                                new AliasExpression(mapCall, "__expr0", mapCall.toString(), false),
                                false, false),
                            // max(col3) extracted from the mixed expression upper(max(col3))
                            new AggregateWrapperExpression(
                                new AliasExpression(new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("max"), null, asList(e("col3"))), "__expr1"),
                                false, true)
                        ),
                        null),
                    asList(
                        uce("col"),
                        uce("__expr0"),
                        new AliasExpression(
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("upper"), null, asList(uce("__expr1"))),
                            "upper(max(col3))")),
                    null);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_lambda_in_projection()
    {
        //@formatter:off
        // Lambda function calls like col2.map(x -> concat(x, 'ggg')) in an aggregate projection
        // must not throw "map is not an aggregate function". SchemaResolver allows them through
        // (since LambdaFunction is exempt from the aggregate-only check) and wraps the resolved
        // FunctionCallExpression in AggregateWrapperExpression. Since there is no aggregate
        // function call inside the lambda, isMixedProjection returns false and the lambda
        // stays in the Aggregate as-is — no outer Projection is added by CEPD.
        String query = "select col, col2.map(x -> concat(x, 'ggg')) "
                + "from \"table\" t "
                + "group by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        // At SchemaResolver stage, lambda body variable 'x' is still UnresolvedColumnExpression
        // with lambdaId=0 (assigned by the parser). ColumnResolver runs later and turns it into
        // a resolved lambda ColumnExpression.
        ILogicalPlan expected =
                new Aggregate(
                    tableScan(Schema.of(ast("t", table)), table),
                    asList(e("col")),
                    asList(
                        new AggregateWrapperExpression(e("col"), false, false),
                        new AggregateWrapperExpression(
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("map"), null,
                                asList(
                                    e("col2"),
                                    new LambdaExpression(asList("x"),
                                        new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("concat"), null,
                                            asList(new UnresolvedColumnExpression(of("x"), 0, null), stringLit("ggg"))),
                                        new int[] { 0 })
                                )),
                            false, false)
                    ),
                    null);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_lambda_in_projection_with_alias()
    {
        //@formatter:off
        // Same as above but with an explicit alias. The aliased case goes through the parser's
        // AggregateWrapperExpression wrapping path (AliasExpression is not IAggregateExpression),
        // so it works differently from the no-alias path but the result in the Aggregate is the same.
        String query = "select col, col2.map(x -> concat(x, 'ggg')) mapped "
                + "from \"table\" t "
                + "group by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Aggregate(
                    tableScan(Schema.of(ast("t", table)), table),
                    asList(e("col")),
                    asList(
                        new AggregateWrapperExpression(e("col"), false, false),
                        new AggregateWrapperExpression(
                            new AliasExpression(
                                new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("map"), null,
                                    asList(
                                        e("col2"),
                                        new LambdaExpression(asList("x"),
                                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("concat"), null,
                                                asList(new UnresolvedColumnExpression(of("x"), 0, null), stringLit("ggg"))),
                                            new int[] { 0 })
                                    )),
                                "mapped"),
                            false, false)
                    ),
                    null);
        //@formatter:on

        assertEquals(expected, actual);
    }

    @Test
    void test_aggregate_chained_lambda_in_projection()
    {
        //@formatter:off
        // Chained lambda calls like col2.filter(x -> x > 10).map(x -> concat(x, 'ggg')) must not
        // throw "concat is not an aggregate function". The inner 'filter' call was resetting
        // insideAggregateFunction to false after resolving its own arguments, which caused
        // 'concat' (in map's lambda) to be rejected. Fix: save/restore the flag per call.
        String query = "select col, col2.filter(x -> x > 10).map(x -> concat(x, 'ggg')) "
                + "from \"table\" t "
                + "group by col ";
        //@formatter:on

        ILogicalPlan plan = getSchemaResolvedPlan(query);
        ILogicalPlan actual = optimize(context, plan);

        //@formatter:off
        ILogicalPlan expected =
                new Aggregate(
                    tableScan(Schema.of(ast("t", table)), table),
                    asList(e("col")),
                    asList(
                        new AggregateWrapperExpression(e("col"), false, false),
                        new AggregateWrapperExpression(
                            new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("map"), null,
                                asList(
                                    new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("filter"), null,
                                        asList(
                                            e("col2"),
                                            new LambdaExpression(asList("x"),
                                                gt(new UnresolvedColumnExpression(of("x"), 0, null), intLit(10)),
                                                new int[] { 0 })
                                        )),
                                    new LambdaExpression(asList("x"),
                                        new FunctionCallExpression("sys", SystemCatalog.get().getScalarFunction("concat"), null,
                                            asList(new UnresolvedColumnExpression(of("x"), 0, null), stringLit("ggg"))),
                                        new int[] { 0 })
                                )),
                            false, false)
                    ),
                    null);
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        assertEquals(expected, actual);
    }

    private ILogicalPlan optimize(IExecutionContext context, ILogicalPlan plan)
    {
        ALogicalPlanOptimizer.Context ctx = optimizer.createContext(context);
        return optimizer.optimize(ctx, plan);
    }
}
