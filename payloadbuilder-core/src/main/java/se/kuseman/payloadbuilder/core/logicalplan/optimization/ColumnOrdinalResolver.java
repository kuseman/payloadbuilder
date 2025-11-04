package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.stream.Collectors.toList;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;

/** Fixes wrong column ordinals */
class ColumnOrdinalResolver extends ALogicalPlanOptimizer<ColumnOrdinalResolver.Ctx>
{
    ColumnOrdinalResolver()
    {
        // No expression rewriting here
        super(null);
    }

    static class Ctx extends ALogicalPlanOptimizer.Context
    {
        Ctx(IExecutionContext context)
        {
            super(context);
        }

        Schema schema;
        Schema outerSchema;
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

    @Override
    public ILogicalPlan visit(Projection plan, Ctx context)
    {
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        // CSOFF
        Schema prevSchema = context.schema;
        Schema prevOuterSchema = context.outerSchema;
        // CSON

        context.schema = input.getSchema();
        context.outerSchema = SchemaUtils.joinSchema(context.outerSchema, context.schema);

        List<IExpression> expressions = plan.getExpressions()
                .stream()
                .map(e -> ColumnOrdinalRewriter.INSTANCE.visit(e, context))
                .collect(toList());

        context.schema = prevSchema;
        context.outerSchema = prevOuterSchema;

        return new Projection(input, expressions, plan.getParentTableSource());
    }

    @Override
    public ILogicalPlan visit(Filter plan, Ctx context)
    {
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        context.schema = input.getSchema();

        IExpression e = ColumnOrdinalRewriter.INSTANCE.visit(plan.getPredicate(), context);
        return new Filter(input, plan.getTableSource(), e);
    }

    @Override
    public ILogicalPlan visit(Sort plan, Ctx context)
    {
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        context.schema = input.getSchema();

        List<SortItem> sortItems = plan.getSortItems()
                .stream()
                .map(si ->
                {
                    IExpression e = ColumnOrdinalRewriter.INSTANCE.visit(si.getExpression(), context);
                    return new SortItem(e, si.getOrder(), si.getNullOrder(), si.getLocation());
                })
                .collect(toList());

        return new Sort(input, sortItems);
    }

    @Override
    public ILogicalPlan visit(Join plan, Ctx context)
    {
        ILogicalPlan outer = plan.getOuter()
                .accept(this, context);

        Schema prevOuterSchema = context.outerSchema;

        // This join has outer references, concat outer schemas
        if (!plan.getOuterReferences()
                .isEmpty())
        {
            context.outerSchema = SchemaUtils.joinSchema(context.outerSchema, outer.getSchema());
        }

        ILogicalPlan inner = plan.getInner()
                .accept(this, context);

        IExpression condition = plan.getCondition();
        if (condition != null)
        {
            // Join condition uses the joined schema
            context.schema = plan.getSchema();

            condition = ColumnOrdinalRewriter.INSTANCE.visit(condition, context);
        }

        context.outerSchema = prevOuterSchema;

        return new Join(outer, inner, plan.getType(), plan.getPopulateAlias(), condition, plan.getOuterReferences(), plan.isSwitchedInputs(), plan.getOuterSchema());
    }

    @Override
    public ILogicalPlan visit(Aggregate plan, Ctx context)
    {
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        context.schema = input.getSchema();
        List<IExpression> aggregateExpressions = plan.getAggregateExpressions()
                .stream()
                .map(e -> ColumnOrdinalRewriter.INSTANCE.visit(e, context))
                .collect(toList());
        List<IAggregateExpression> projectionExpressions = plan.getProjectionExpressions()
                .stream()
                .map(e -> (IAggregateExpression) ColumnOrdinalRewriter.INSTANCE.visit(e, context))
                .collect(toList());
        return new Aggregate(input, aggregateExpressions, projectionExpressions, plan.getParentTableSource());
    }

    @Override
    public ILogicalPlan visit(TableFunctionScan plan, Ctx context)
    {
        context.schema = null;
        List<IExpression> arguments = plan.getArguments()
                .stream()
                .map(e -> ColumnOrdinalRewriter.INSTANCE.visit(e, context))
                .collect(toList());

        return new TableFunctionScan(plan.getTableSource(), plan.getSchema(), arguments, plan.getOptions(), plan.getLocation());
    }

    @Override
    public ILogicalPlan visit(ExpressionScan plan, Ctx context)
    {
        context.schema = null;
        IExpression expression = ColumnOrdinalRewriter.INSTANCE.visit(plan.getExpression(), context);
        return new ExpressionScan(plan.getTableSource(), plan.getSchema(), expression, plan.getLocation());
    }

    static class ColumnOrdinalRewriter extends ARewriteExpressionVisitor<Ctx>
    {
        private static final ColumnOrdinalRewriter INSTANCE = new ColumnOrdinalRewriter();

        @Override
        public IExpression visit(IColumnExpression expression, Ctx context)
        {
            ColumnExpression ce = (ColumnExpression) expression;
            int ordinal = ce.getOrdinal();
            if (ordinal >= 0)
            {
                ColumnReference cr = ce.getColumnReference();

                TableSourceReference tableSource = cr != null ? cr.tableSourceReference()
                        : null;
                int tableSourceId = tableSource != null ? tableSource.getId()
                        : -1;
                String alias = ce.getAlias()
                        .getAlias();

                Schema schema = context.schema;
                if (ce.isOuterReference())
                {
                    schema = context.outerSchema;
                }

                int size = schema.getSize();
                for (int i = 0; i < size; i++)
                {
                    Column schemaColumn = schema.getColumns()
                            .get(i);
                    TableSourceReference schemaTableSource = SchemaUtils.getTableSource(schemaColumn);
                    int schemaTableSourceId = schemaTableSource != null ? schemaTableSource.getId()
                            : -1;
                    if (tableSourceId == schemaTableSourceId
                            && schemaColumn.getName()
                                    .equalsIgnoreCase(alias)
                            && ordinal != i)
                    {
                        return new ColumnExpression(ce, i);
                    }
                }
            }

            return expression;
        }
    }
}
