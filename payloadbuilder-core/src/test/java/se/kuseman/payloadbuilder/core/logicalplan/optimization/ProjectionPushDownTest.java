package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.List;
import java.util.Random;
import java.util.Set;

import org.assertj.core.api.Assertions;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.DereferenceExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference.ColumnReference;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Test of {@link ProjectionPushDown} */
public class ProjectionPushDownTest extends ALogicalPlanOptimizerTest
{
    private final ColumnResolver columnOptimizer = new ColumnResolver();
    private final ProjectionPushDown projectionPushDown = new ProjectionPushDown();
    private final Schema schema = Schema.of(ast("t", Type.Any, table));
    private final Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
    private final Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

    @Test
    public void test_projection_on_one_table_from_join()
    {
        //@formatter:off
        String query = """
                select b.col2, b.col3
                from tableA p
                inner populate join tableB b
                  on b.col = p.col
                """;
        //@formatter:on

        ILogicalPlan actual = optimize(getSchemaResolvedPlan(query));
        TableSourceReference tableA = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableA"), "p");
        TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");

        Schema schemaA = Schema.of(ast("p", tableA));
        Schema schemaB = Schema.of(ast("b", tableB));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                        tableScan(schemaA, tableA, List.of("col")),
                        tableScan(schemaB, tableB, List.of("col", "col2", "col3")),
                        Join.Type.INNER,
                        "b",
                        eq(cre("col", tableB), cre("col", tableA)),
                        null,
                        false,
                        Schema.EMPTY),
                    List.of(new DereferenceExpression(cre("b", tableB, ResolvedType.table(schemaB), CoreColumn.Type.POPULATED), "col2", -1, ResolvedType.array(Type.Any),
                                new ColumnReference(tableB, CoreColumn.Type.REGULAR)),
                            new DereferenceExpression(cre("b", tableB, ResolvedType.table(schemaB), CoreColumn.Type.POPULATED), "col3", -1, ResolvedType.array(Type.Any),
                                new ColumnReference(tableB, CoreColumn.Type.REGULAR)))
                );
        //@formatter:on
        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    @Test
    public void test_asterisk_projection()
    {
        //@formatter:off
        String query = """
                select *
                from "table" t
                """;
        //@formatter:on

        ILogicalPlan actual = optimize(getSchemaResolvedPlan(query));

        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "t");
        Schema schema = Schema.of(ast("t", table));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    tableScan(schema, table),
                    List.of(new AsteriskExpression(QualifiedName.of(), null, Set.of(table)))
                );
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
    public void test_asterisk_projection_in_correlated_sub_query()
    {
        //@formatter:off
        String query = """
                select *
                from "table" t
                cross apply (
                    select t.col
                ) x
                """;
        //@formatter:on

        ILogicalPlan actual = optimize(getSchemaResolvedPlan(query));

        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "t");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schema = Schema.of(ast("t", table));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                        tableScan(schema, table),
                        subQuery(
                            ConstantScan.create(subQueryX, List.of("col"), List.of(List.of(ocre("col", table))), null),
                            subQueryX),
                        Join.Type.INNER,
                        null,
                        null,
                        Set.of(col("col", ResolvedType.ANY, table)),
                        false,
                        Schema.of(ast("t", table))),
                    List.of(new AsteriskExpression(QualifiedName.of(), null, Set.of(table))),
                    null);
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
    public void test_asterisk_projection_sub_query_with_no_projection_on_table_source()
    {
        // Verify that projection is not set when we have an asterisk projection above in hierarchy
        //@formatter:off
        String query = """
                select x.*
                from "table" t
                cross apply (
                    select 1 col
                ) x
                """;
        //@formatter:on

        ILogicalPlan actual = optimize(getSchemaResolvedPlan(query));

        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "t");
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");
        Schema schema = Schema.of(ast("t", table));

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    new Join(
                        // No projected columns here
                        tableScanNoProjection(schema, table),
                        subQuery(
                            ConstantScan.create(subQueryX, List.of("col"), List.of(List.of(new AliasExpression(intLit(1), "col"))), null),
                            subQueryX),
                        Join.Type.INNER,
                        null,
                        null,
                        Set.of(),
                        false,
                        Schema.of(ast("t", table))),
                    List.of(cre("col", subQueryX, ResolvedType.INT)),
                    null);
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
    public void test_mixed_asterisk_with_column_projections()
    {
        // Verify that we don't recreate table source with projected columns here
        // since we have an asterisk select and hence we need all column not only "col"
        //@formatter:off
        String query = """
                select t.col, t.*, t.col2
                from "table" t
                """;
        //@formatter:on

        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "t");
        // ColumnReference tAst = new ColumnReference(table, "t", ColumnReference.Type.ASTERISK);
        Schema schema = Schema.of(ast("t", table));

        ILogicalPlan actual = optimize(getSchemaResolvedPlan(query));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    tableScan(schema, table),
                asList(cre("col", table), new AsteriskExpression(QualifiedName.of("t"), null, Set.of(table)), cre("col2", table)));
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
    public void test_nested_projections()
    {
        /*@formatter:off
         * 
         * select x.col1
         * from
         * (
         *   select t.col1, t.col2
         *   from table t
         * ) x
         * 
         *
         *@formatter:on
         */
        TableSourceReference subQueryX = new TableSourceReference(1, TableSourceReference.Type.SUBQUERY, "", QualifiedName.of("x"), "x");

        //@formatter:off
        ILogicalPlan plan =
                projection(
                    subQuery(
                        projection(
                            tableScan(schema, table),
                            asList(e("t.col1"), e("t.col2"))),
                    subQueryX),
                    asList(e("x.col1"))
                );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected =
                projection(
                    subQuery(
                        projection(
                            tableScan(schema, table, asList("col1", "col2")),
                            asList(cre("col1", table), cre("col2", table)),
                            subQueryX),
                    subQueryX),
                    asList(cre("col1", table, 0))
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
    public void test_single_projection_and_table()
    {
        //@formatter:off
        ILogicalPlan plan = projection(
                tableScan(schema, table),
                asList(e("t.col1"))
                );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
        ILogicalPlan expected = projection(
                tableScan(schema, table, asList("col1")),
                asList(cre("col1", table))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
        assertEquals(expected, actual);
    }

    @Test
    public void test_correlated_join_without_asterisk()
    {
        ScalarFunctionInfo toTableFunc = SystemCatalog.get()
                .getScalarFunction("totable");

        // Verify that we get correlated projections gathered
        //@formatter:off
        String query = """
                select t.col1, x.col2
                from "table" t
                cross apply (totable(t."table")) x
                """;
        //@formatter:on

        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "t");
        Schema schema = Schema.of(ast("t", table));

        TableSourceReference toTable = new TableSourceReference(1, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("totable(t.table)"), "x");
        Schema toTableSchema = Schema.of(ast("x", toTable));

        ILogicalPlan actual = optimize(getSchemaResolvedPlan(query));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        tableScan(schema, table, List.of("col1", "table")),
                        new ExpressionScan(toTable, toTableSchema, new FunctionCallExpression("sys", toTableFunc, null, asList(ocre("table", table))), null),
                        Join.Type.INNER,
                        null,
                        null,
                        Set.of(col("table", ResolvedType.ANY, table)),
                        false,
                        Schema.of(ast("t", table))),
                    asList(cre("col1", table), cre("col2", toTable))
                );
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
    public void test_correlated_join_with_asterisk()
    {
        ScalarFunctionInfo toTableFunc = SystemCatalog.get()
                .getScalarFunction("totable");

        // Verify that we get correlated projections gathered
        //@formatter:off
        String query = """
                select t.col1, x.col2, t.*
                from "table" t
                cross apply (totable(t."table")) x
                """;
        //@formatter:on

        TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "t");
        Schema schema = Schema.of(ast("t", table));

        TableSourceReference toTable = new TableSourceReference(1, TableSourceReference.Type.EXPRESSION, "", QualifiedName.of("totable(t.table)"), "x");
        Schema toTableSchema = Schema.of(ast("x", toTable));

        ILogicalPlan actual = optimize(getSchemaResolvedPlan(query));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    new Join(
                        tableScan(schema, table),
                        new ExpressionScan(toTable, toTableSchema, new FunctionCallExpression("sys", toTableFunc, null, asList(ocre("table", table))), null),
                        Join.Type.INNER,
                        null,
                        null,
                        Set.of(col("table", ResolvedType.ANY, table)),
                        false,
                        Schema.of(ast("t", table))),
                    asList(cre("col1", table), cre("col2", toTable), new AsteriskExpression(QualifiedName.of("t"), null, Set.of(table))
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
    public void test_join()
    {
        //@formatter:off
        /*
         * select a.col1, b.col2
         * from tableA a
         * inner join tableB b
         *  on a.col3 = b.col4
         */
        ILogicalPlan plan = 
                projection(
                    new Sort(
                        new Filter(
                            new Join(
                                    tableScan(schemaA, tableA),
                                    tableScan(schemaB, tableB),
                                    Join.Type.INNER,
                                    null,
                                    e("a.col3 = b.col4"),
                                    asSet(),
                                    false,
                                    Schema.EMPTY),
                            null,
                            e("a.col7 > b.col8")
                        ),
                        asList(new SortItem(e("a.col5"), Order.ASC, NullOrder.UNDEFINED, null), new SortItem(e("b.col6"), Order.ASC, NullOrder.UNDEFINED, null))),
                    asList(e("a.col1"), e("b.col2"))
                );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

        //@formatter:off
         ILogicalPlan expected = 
                 projection(
                     new Sort(
                         new Filter(
                             new Join(
                                 tableScan(schemaA, tableA, asList("col1", "col3", "col5", "col7")),
                                 tableScan(schemaB, tableB, asList("col2", "col4", "col6", "col8")),
                                 Join.Type.INNER,
                                 null,
                                 eq(cre("col3", tableA), cre("col4", tableB)),
                                 asSet(),
                                 false,
                                 Schema.EMPTY),
                             null,
                             gt(cre("col7", tableA), cre("col8", tableB))
                         ),
                         asList(new SortItem(cre("col5", tableA), Order.ASC, NullOrder.UNDEFINED, null), new SortItem(cre("col6", tableB), Order.ASC, NullOrder.UNDEFINED, null))),
                     asList(cre("col1", tableA), cre("col2", tableB)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expected);

        assertEquals(expected, actual);
    }

    /**
     * Optimizes plan with projection push down. Expressions needs to be resolved before we can analyze predicates to run it through the {@link ColumnResolver} first.
     */
    private ILogicalPlan optimize(ILogicalPlan plan)
    {
        ALogicalPlanOptimizer.Context ctx = columnOptimizer.createContext(context);
        plan = columnOptimizer.optimize(ctx, plan);
        ctx = projectionPushDown.createContext(context);
        return projectionPushDown.optimize(ctx, plan);
    }
}
