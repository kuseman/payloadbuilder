package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;
import static java.util.Collections.unmodifiableList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;

/** Gathers projection columns and pushes them down to scan operators */
class ProjectionPushDown extends ALogicalPlanOptimizer<ProjectionPushDown.Ctx>
{
    private static final Set<String> ASTERISK_PROJECTION = emptySet();

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

        /** Collect phase, first run we collect columns only and don't re-create table scans */
        boolean collect = true;
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
        Ctx ctx = (Ctx) context;

        // First we only traverse and collect columns
        plan.accept(this, ctx);

        ctx.collect = false;

        return plan.accept(this, ctx);

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
        if (!context.collect)
        {
            return expression;
        }

        visit(plan, singletonList(expression), context);
        return expression;
    }

    @Override
    protected List<IExpression> visit(ILogicalPlan plan, List<IExpression> expressions, Ctx context)
    {
        if (!context.collect)
        {
            return expressions;
        }

        for (IExpression expression : expressions)
        {
            // Asterisk projection, mark table sources
            if (expression instanceof AsteriskExpression ae
                    && ae.getQname()
                            .size() == 0)
            {
                for (TableSourceReference tsr : ae.getTableSources())
                {
                    context.columnsByAlias.put(tsr, ASTERISK_PROJECTION);
                }
            }
        }

        Set<ColumnReference> columns = collectColumns(expressions);
        List<ColumnReference> colRefs = new ArrayList<>();
        for (ColumnReference extractedColumn : columns)
        {
            colRefs.clear();
            ColumnReference cr = extractedColumn;
            while (cr != null)
            {
                colRefs.add(cr);
                cr = cr.getLinkedColumnReference();
            }

            for (ColumnReference colRef : colRefs)
            {
                TableSourceReference tableSource = colRef.getTableSource();
                Set<String> projectionColumns = context.columnsByAlias.computeIfAbsent(tableSource, k -> new HashSet<>());
                if (projectionColumns == ASTERISK_PROJECTION)
                {
                    continue;
                }

                // Mark that current table source is asterisk and we cannot project any columns
                if (extractedColumn.isAsterisk())
                {
                    context.columnsByAlias.put(tableSource, ASTERISK_PROJECTION);
                    continue;
                }

                // Populated columns aren't real columns so skip those
                if (!extractedColumn.isPopulated())
                {
                    projectionColumns.add(extractedColumn.getName());
                }
            }
        }

        return expressions;
    }

    @Override
    protected TableScan create(TableScan plan, Ctx context)
    {
        // Wrong phase
        if (context.collect)
        {
            return plan;
        }

        Optional<List<String>> projection;
        Set<String> columns = context.columnsByAlias.get(plan.getTableSource());

        // Nothing touched this table source => no projected columns
        // This happens when this table scan is present in tree but nothing is used ie.
        // select b.*
        // from tableA
        // cross apply (
        // select 123 col
        // ) b
        //
        //
        if (columns == null)
        {
            projection = Optional.of(emptyList());
        }
        // This table source has been referenced by an asterisk projection => no specific projection ie. all columns wanted
        else if (columns == ASTERISK_PROJECTION)
        {
            projection = Optional.empty();
        }
        // Only a subset of columns wanted
        else
        {
            List<String> tableColumns = new ArrayList<>(columns);
            tableColumns.sort(String::compareTo);
            projection = Optional.of(unmodifiableList(tableColumns));
        }

        return new TableScan(plan.getTableSchema(), plan.getTableSource(), projection, plan.isTempTable(), plan.getOptions(), plan.getLocation());
    }
}
