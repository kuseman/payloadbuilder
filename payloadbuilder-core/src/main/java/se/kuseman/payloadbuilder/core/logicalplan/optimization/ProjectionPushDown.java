package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptySet;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn.Type;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.AsteriskExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;

/** Gathers projection columns and pushes them down to scan operators */
class ProjectionPushDown extends ALogicalPlanOptimizer<ProjectionPushDown.Ctx>
{
    private static final Logger LOGGER = LoggerFactory.getLogger(ProjectionPushDown.class);

    ProjectionPushDown()
    {
        // No expression rewrite here
        super(null);
    }

    static class Ctx extends ALogicalPlanOptimizer.Context
    {
        /** Marker for qualified asterisk references. */
        private static final Set<String> ASTERISK_PROJECTION = emptySet();

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
            if (expression instanceof AggregateWrapperExpression awe)
            {
                expression = awe.getExpression();
            }

            // Asterisk projection, mark table sources
            if (expression instanceof AsteriskExpression ae)
            {
                for (TableSourceReference tsr : ae.getTableSourceReferences())
                {
                    TableSourceReference sr = tsr;
                    while (sr != null)
                    {
                        context.columnsByAlias.put(sr, Ctx.ASTERISK_PROJECTION);
                        sr = sr.getParent();
                    }
                }
            }
        }

        Map<TableSourceReference, Set<ColumnReferenceExtractorResult>> columns = collectColumns(expressions);
        for (Entry<TableSourceReference, Set<ColumnReferenceExtractorResult>> e : columns.entrySet())
        {
            for (ColumnReferenceExtractorResult ce : e.getValue())
            {
                TableSourceReference tableSource = ce.columnReference()
                        .tableSourceReference()
                        .getRoot();

                // Populated column should not be projected since they are not real columns
                // But instead go into the populated schema and add those columns if they are static
                if (ce.expression() instanceof ColumnExpression cexp
                        && cexp.getColumnType() == Type.POPULATED)
                {
                    if (ce.expression()
                            .getType()
                            .getType() == Column.Type.Table)
                    {
                        for (Column col : ce.expression()
                                .getType()
                                .getSchema()
                                .getColumns())
                        {
                            if (SchemaUtils.isAsterisk(col))
                            {
                                // Mark this table source as an asterisk projection
                                context.columnsByAlias.put(tableSource, Ctx.ASTERISK_PROJECTION);
                                break;
                            }
                            appendColumn(tableSource, col.getName(), context);
                        }
                    }
                    continue;
                }

                appendColumn(tableSource, ce.columnName(), context);
            }
        }

        return expressions;
    }

    private void appendColumn(TableSourceReference tableSource, String column, Ctx context)
    {
        Set<String> projectionColumns = context.columnsByAlias.computeIfAbsent(tableSource, k -> new HashSet<>());
        // If the table source is already tagged with ALL (asterisks)
        // then it doesn't matter if there is a real column reference, we need every thing anyway
        if (projectionColumns != Ctx.ASTERISK_PROJECTION)
        {
            projectionColumns.add(column);
        }
    }

    @Override
    protected TableScan create(TableScan plan, Ctx context)
    {
        // Wrong phase
        if (context.collect)
        {
            return plan;
        }

        Set<String> columns = context.columnsByAlias.get(plan.getTableSource());
        Projection projection = Projection.NONE;
        if (columns == Ctx.ASTERISK_PROJECTION)
        {
            projection = Projection.ALL;
        }
        else if (columns != null)
        {
            List<String> tableColumns = new ArrayList<>(columns);
            tableColumns.sort(String::compareTo);
            projection = Projection.columns(tableColumns);
        }

        LOGGER.debug("Projected columns for {}: {}", plan.getTableSource(), projection);
        return new TableScan(plan.getTableSchema(), plan.getTableSource(), projection, plan.getOptions(), plan.getLocation());
    }
}
