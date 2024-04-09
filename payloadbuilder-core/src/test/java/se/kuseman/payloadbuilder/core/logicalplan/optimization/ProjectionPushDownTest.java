package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.List;
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
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.catalog.system.SystemCatalog;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.parser.Location;

/** Test of {@link ProjectionPushDown} */
public class ProjectionPushDownTest extends ALogicalPlanOptimizerTest
{
    private final ColumnResolver columnOptimizer = new ColumnResolver();
    private final ProjectionPushDown projectionPushDown = new ProjectionPushDown();

    // CSOFF
    private final ColumnReference tCol = new ColumnReference(table, "t", ColumnReference.Type.ASTERISK);
    private final ColumnReference aCol = new ColumnReference(tableA, "a", ColumnReference.Type.ASTERISK);
    private final ColumnReference bCol = new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK);
    // CSON

    private final Schema schema = Schema.of(col(tCol, Type.Any));
    private final Schema schemaA = Schema.of(col(aCol, Type.Any));
    private final Schema schemaB = Schema.of(col(bCol, Type.Any));

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

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "t");
        ColumnReference tAst = new ColumnReference(table, "t", ColumnReference.Type.ASTERISK);
        Schema schema = Schema.of(col(tAst, Type.Any));

        ILogicalPlan expected = new Projection(tableScan(schema, table), List.of(new AsteriskExpression(QualifiedName.of(), null, Set.of(table))), false);

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
        // Verify that projection is not set when we have an asterisk projection above in hierarchy
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

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "t");
        ColumnReference tAst = new ColumnReference(table, "t", ColumnReference.Type.ASTERISK);
        Schema schema = Schema.of(col(tAst, Type.Any));

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    new Join(
                        tableScan(schema, table),
                        projection(ConstantScan.INSTANCE, asList(
                            ocre(tAst.rename("col"))
                        )),
                        Join.Type.INNER,
                        null,
                        null,
                        Set.of(CoreColumn.of(tAst.rename("col"), ResolvedType.of(Type.Any))),
                        false),
                    List.of(new AsteriskExpression(QualifiedName.of(), null, Set.of(table))),
                    false);
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

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "t");
        ColumnReference tAst = new ColumnReference(table, "t", ColumnReference.Type.ASTERISK);
        Schema schema = Schema.of(col(tAst, Type.Any));

        //@formatter:off
        ILogicalPlan expected =
                new Projection(
                    new Join(
                        tableScan(schema, table, List.of()),
                        projection(ConstantScan.INSTANCE, asList(
                            new AliasExpression(intLit(1), "col")
                        )),
                        Join.Type.INNER,
                        null,
                        null,
                        Set.of(),
                        false),
                    List.of(ce("col", ResolvedType.of(Type.Int))),
                    false);
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

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "t");
        ColumnReference tAst = new ColumnReference(table, "t", ColumnReference.Type.ASTERISK);
        Schema schema = Schema.of(col(tAst, Type.Any));

        ILogicalPlan actual = optimize(getSchemaResolvedPlan(query));

        //@formatter:off
        ILogicalPlan expected = 
                projection(
                    tableScan(schema, table),
                asList(cre(tAst.rename("col")), cre(tAst), cre(tAst.rename("col2")))
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

        //@formatter:off
        ILogicalPlan plan = projection(
                subQuery(
                    projection(
                        tableScan(schema, table),
                        asList(e("t.col1"), e("t.col2"))),
                    "x"),
                asList(e("x.col1"))
                );
        //@formatter:on

        ILogicalPlan actual = optimize(plan);

      //@formatter:off
        ILogicalPlan expected =
                projection(
                    tableScan(schema, table, asList("col1")),
                asList(cre(tCol.rename("col1")))
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
                asList(cre(tCol.rename("col1")))
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

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "t");
        ColumnReference tAst = new ColumnReference(table, "t", ColumnReference.Type.ASTERISK);
        Schema schema = Schema.of(col(tAst, Type.Any));

        TableSourceReference toTable = new TableSourceReference(1, "", QualifiedName.of("totable(t.table)"), "x");
        ColumnReference toTableAst = new ColumnReference(toTable, "x", ColumnReference.Type.ASTERISK);
        Schema toTableSchema = Schema.of(col(toTableAst, Type.Any));

        ILogicalPlan actual = optimize(getSchemaResolvedPlan(query));

        //@formatter:off
        ILogicalPlan expected = projection(
                new Join(
                        tableScan(schema, table, asList("col1", "table")),
                        new ExpressionScan(toTable, toTableSchema, new FunctionCallExpression("sys", toTableFunc, null, asList(ocre(tAst.rename("table")))), null),
                        Join.Type.INNER,
                        null,
                        null,
                        Set.of(CoreColumn.of(tAst.rename("table"), ResolvedType.of(Type.Any))),
                        false),
                asList(cre(tAst.rename("col1")), cre(toTableAst.rename("col2")))
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

        TableSourceReference table = new TableSourceReference(0, "", QualifiedName.of("table"), "t");
        ColumnReference tAst = new ColumnReference(table, "t", ColumnReference.Type.ASTERISK);
        Schema schema = Schema.of(col(tAst, Type.Any));

        TableSourceReference toTable = new TableSourceReference(1, "", QualifiedName.of("totable(t.table)"), "x");
        ColumnReference toTableAst = new ColumnReference(toTable, "x", ColumnReference.Type.ASTERISK);
        Schema toTableSchema = Schema.of(col(toTableAst, Type.Any));

        ILogicalPlan actual = optimize(getSchemaResolvedPlan(query));

        //@formatter:off
        ILogicalPlan expected = projection(
                new Join(
                        tableScan(schema, table),
                        new ExpressionScan(toTable, toTableSchema, new FunctionCallExpression("sys", toTableFunc, null, asList(ocre(tAst.rename("table")))), null),
                        Join.Type.INNER,
                        null,
                        null,
                        Set.of(CoreColumn.of(tAst.rename("table"), ResolvedType.of(Type.Any))),
                        false),
                asList(cre(tAst.rename("col1")), cre(toTableAst.rename("col2")), cre(tAst))
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
    public void test_join()
    {
        //@formatter:off
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
                                    false),
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
                                 eq(cre(aCol.rename("col3")), cre(bCol.rename("col4"))),
                                 asSet(),
                                 false),
                             null,
                             gt(cre(aCol.rename("col7")), cre(bCol.rename("col8")))
                         ),
                         asList(new SortItem(cre(aCol.rename("col5")), Order.ASC, NullOrder.UNDEFINED, null), new SortItem(cre(bCol.rename("col6")), Order.ASC, NullOrder.UNDEFINED, null))),
                     asList(cre(aCol.rename("col1")), cre(bCol.rename("col2"))));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));

        Assertions.assertThat(actual)
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class)
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
