package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;

/** Test of {@link ProjectionPushDown} */
public class ProjectionPushDownTest extends ALogicalPlanOptimizerTest
{
    private final ColumnResolver columnOptimizer = new ColumnResolver();
    private final ProjectionPushDown projectionPushDown = new ProjectionPushDown();
    private final Schema schema = Schema.of(ast("t", Type.Any, table));
    private final Schema schemaA = Schema.of(ast("a", Type.Any, tableA));
    private final Schema schemaB = Schema.of(ast("b", Type.Any, tableB));

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
                asList(cre("col1", table))
                );
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
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
                                 eq(cre("col3", tableA), cre("col4", tableB)),
                                 asSet(),
                                 false),
                             null,
                             gt(cre("col7", tableA), cre("col8", tableB))
                         ),
                         asList(new SortItem(cre("col5", tableA), Order.ASC, NullOrder.UNDEFINED, null), new SortItem(cre("col6", tableB), Order.ASC, NullOrder.UNDEFINED, null))),
                     asList(cre("col1", tableA), cre("col2", tableB)));
        //@formatter:on

        // System.out.println(expected.print(0));
        // System.out.println(actual.print(0));
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
