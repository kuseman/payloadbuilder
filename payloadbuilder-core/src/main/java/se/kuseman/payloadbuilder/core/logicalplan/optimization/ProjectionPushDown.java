package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;

/** Gathers projection columns and pushes them down to scan operators */
class ProjectionPushDown extends ALogicalPlanOptimizer<ProjectionPushDown.Ctx>
{
    ProjectionPushDown()
    {
        // No expression rewrite here
        super(null);
    }

    static class Ctx extends ALogicalPlanOptimizer.Context
    {
        Ctx(IExecutionContext context)
        {
            super(context);
        }

        Map<TableSourceReference, Set<String>> columnsByAlias = new HashMap<>();
    }

    @Override
    Ctx createContext(IExecutionContext context)
    {
        return new Ctx(context);
    }

    @Override
    ILogicalPlan optimize(Context context, ILogicalPlan plan)
    {
        return plan.accept(this, (Ctx) context);
    }

    /*
     * @formatter:off
     * 
     * select col1, col2
     * from table
     * 
     * projection (col1, col2)
     *   scan (table, projection: []);
     *   
     * ==>
     * 
     * projection (col1, col2)
     *   scan(table, projection: [col1, col2])
     * 
     * ---------------- 
     * 
     * projection (a.col1, b.col2)
     *   join (a.col3 = b.col4)
     *     scan (a: tableA, projection: [])
     *     scan (b: tableB, projection: [])
     * 
     * ==>
     * 
     * A nested column (key) is only interested in populated joins
     * In a non populated joins this means we will reflect that value runtime
     * but the projection should still be col1 for tableA
     * 
     * projection (a.col1.key, b.col2)
     *   join (a.col3 = b.col4)
     *     scan (a: tableA, projection: [col1, col3])
     *     scan (b: tableB, projection: [col2, col4])
     * 
     * @formatter:on
     */

    @Override
    protected IExpression visit(ILogicalPlan plan, IExpression expression, Ctx context)
    {
        visit(plan, singletonList(expression), context);
        return expression;
    }

    @Override
    protected List<IExpression> visit(ILogicalPlan plan, List<IExpression> expressions, Ctx context)
    {
        Map<TableSourceReference, Set<String>> columns = collectColumns(expressions);

        for (Entry<TableSourceReference, Set<String>> e : columns.entrySet())
        {
            context.columnsByAlias.computeIfAbsent(e.getKey(), k -> new HashSet<>())
                    .addAll(e.getValue());
        }

        return expressions;
    }

    @Override
    protected TableScan create(TableScan plan, Ctx context)
    {
        Set<String> columns = context.columnsByAlias.get(plan.getTableSource());
        if (columns != null)
        {
            List<String> tableColumns = new ArrayList<>(columns);
            tableColumns.sort(String::compareTo);
            return new TableScan(plan.getTableSchema(), plan.getTableSource(), tableColumns, plan.isTempTable(), plan.getOptions(), plan.getLocation());
        }
        return plan;
    }
}
