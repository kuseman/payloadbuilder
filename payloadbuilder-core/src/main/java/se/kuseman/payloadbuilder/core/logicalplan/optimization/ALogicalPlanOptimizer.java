package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptyMap;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.expression.AExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedColumnExpression;
import se.kuseman.payloadbuilder.core.logicalplan.ALogicalPlanVisitor;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.Concatenation;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.ExpressionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.Join;
import se.kuseman.payloadbuilder.core.logicalplan.Limit;
import se.kuseman.payloadbuilder.core.logicalplan.MaxRowCountAssert;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.Projection;
import se.kuseman.payloadbuilder.core.logicalplan.Sort;
import se.kuseman.payloadbuilder.core.logicalplan.SubQuery;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;

/**
 * Base class for logical plan optimization rules. This visitor walks the tree and recreates each node if wanted enabling extended classes to apply rules to certain nodes
 */
abstract class ALogicalPlanOptimizer<C extends ALogicalPlanOptimizer.Context> extends ALogicalPlanVisitor<ILogicalPlan, C>
{
    private final ARewriteExpressionVisitor<C> expressionRewriter;

    ALogicalPlanOptimizer(ARewriteExpressionVisitor<C> expressionRewriter)
    {
        this.expressionRewriter = expressionRewriter;
    }

    static class Context
    {
        final IExecutionContext context;
        Map<QualifiedName, TableSchema> schemaByTempTable = emptyMap();
        Map<TableSourceReference, TableSchema> schemaByTableSource = new HashMap<>();
        // Counter used to generate unique expression column names for each pushed down sub expression
        int expressionCounter;

        Context(IExecutionContext context)
        {
            this.context = requireNonNull(context, "context");
        }

        QuerySession getSession()
        {
            return (QuerySession) context.getSession();
        }
    }

    /** Optimize input plan */
    abstract ILogicalPlan optimize(Context context, ILogicalPlan plan);

    /** Create context for this optimizer */
    abstract C createContext(IExecutionContext context);

    @Override
    public ILogicalPlan visit(TableScan plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }

        return plan;
    }

    @Override
    public ILogicalPlan visit(TableFunctionScan plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }

        return plan;
    }

    @Override
    public ILogicalPlan visit(ExpressionScan plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }

        return plan;
    }

    @Override
    public ILogicalPlan visit(Projection plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Aggregate plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Join plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Filter plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Sort plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(SubQuery plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Limit plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(OperatorFunctionScan plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(ConstantScan plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(MaxRowCountAssert plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    @Override
    public ILogicalPlan visit(Concatenation plan, C context)
    {
        if (preVisit(plan, context))
        {
            return create(plan, context);
        }
        return plan;
    }

    protected IExpression visit(ILogicalPlan plan, IExpression expression, C context)
    {
        return expression;
    }

    protected List<IExpression> visit(ILogicalPlan plan, List<IExpression> expressions, C context)
    {
        return expressions;
    }

    protected boolean preVisit(TableScan plan, C context)
    {
        return true;
    }

    protected boolean preVisit(TableFunctionScan plan, C context)
    {
        return true;
    }

    protected boolean preVisit(ExpressionScan plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Projection plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Aggregate plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Join plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Filter plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Sort plan, C context)
    {
        return true;
    }

    protected boolean preVisit(SubQuery plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Limit plan, C context)
    {
        return true;
    }

    protected boolean preVisit(OperatorFunctionScan plan, C context)
    {
        return true;
    }

    protected boolean preVisit(ConstantScan plan, C context)
    {
        return true;
    }

    protected boolean preVisit(MaxRowCountAssert plan, C context)
    {
        return true;
    }

    protected boolean preVisit(Concatenation plan, C context)
    {
        return true;
    }

    protected ILogicalPlan create(TableScan plan, C context)
    {
        List<Option> options = plan.getOptions();

        if (expressionRewriter != null)
        {
            options = options.stream()
                    .map(o -> new Option(o.getOption(), o.getValueExpression()
                            .accept(expressionRewriter, context)))
                    .collect(toList());
        }
        else
        {
            options = options.stream()
                    .map(o -> new Option(o.getOption(), visit(plan, o.getValueExpression(), context)))
                    .collect(toList());
        }

        return new TableScan(plan.getTableSchema(), plan.getTableSource(), plan.getProjection(), plan.isTempTable(), options, plan.getLocation());
    }

    protected ILogicalPlan create(TableFunctionScan plan, C context)
    {
        List<IExpression> arguments = plan.getArguments();
        List<Option> options = plan.getOptions();

        if (expressionRewriter != null)
        {
            arguments = arguments.stream()
                    .map(e -> e.accept(expressionRewriter, context))
                    .collect(toList());

            options = options.stream()
                    .map(o -> new Option(o.getOption(), o.getValueExpression()
                            .accept(expressionRewriter, context)))
                    .collect(toList());
        }
        else
        {
            arguments = visit(plan, arguments, context);
            options = options.stream()
                    .map(o -> new Option(o.getOption(), visit(plan, o.getValueExpression(), context)))
                    .collect(toList());
        }
        return new TableFunctionScan(plan.getTableSource(), plan.getSchema(), arguments, options, plan.getLocation());
    }

    protected ILogicalPlan create(ExpressionScan plan, C context)
    {
        IExpression expression = plan.getExpression();

        if (expressionRewriter != null)
        {
            expression = expression.accept(expressionRewriter, context);
        }
        else
        {
            expression = visit(plan, expression, context);
        }

        return new ExpressionScan(plan.getTableSource(), plan.getSchema(), expression, plan.getLocation());
    }

    protected ILogicalPlan create(Projection plan, C context)
    {
        List<IExpression> expressions = plan.getExpressions();

        if (expressionRewriter != null)
        {
            expressions = expressions.stream()
                    .map(e -> e.accept(expressionRewriter, context))
                    .collect(toList());
        }
        else
        {
            expressions = visit(plan, plan.getExpressions(), context);
        }
        return new Projection(plan.getInput()
                .accept(this, context), expressions);
    }

    protected ILogicalPlan create(Aggregate plan, C context)
    {
        List<IExpression> aggregateExpressions = plan.getAggregateExpressions();
        List<IAggregateExpression> projectionExpressions = plan.getProjectionExpressions();
        if (expressionRewriter != null)
        {
            aggregateExpressions = aggregateExpressions.stream()
                    .map(e -> e.accept(expressionRewriter, context))
                    .collect(toList());

            projectionExpressions = projectionExpressions.stream()
                    .map(e -> (IAggregateExpression) e.accept(expressionRewriter, context))
                    .collect(toList());
        }
        else
        {
            aggregateExpressions = visit(plan, aggregateExpressions, context);

            projectionExpressions = projectionExpressions.stream()
                    .map(e -> (IAggregateExpression) visit(plan, e, context))
                    .collect(toList());
        }

        return new Aggregate(plan.getInput()
                .accept(this, context), aggregateExpressions, projectionExpressions);
    }

    protected ILogicalPlan create(Join plan, C context)
    {
        IExpression condition = plan.getCondition();
        if (condition != null)
        {
            if (expressionRewriter != null)
            {
                condition = condition.accept(expressionRewriter, context);
            }
            else
            {
                condition = visit(plan, condition, context);
            }
        }
        return new Join(plan.getOuter()
                .accept(this, context),
                plan.getInner()
                        .accept(this, context),
                plan.getType(), plan.getPopulateAlias(), condition, plan.getOuterReferences(), plan.isSwitchedInputs(), plan.getOuterSchema());
    }

    protected ILogicalPlan create(Sort plan, C context)
    {
        List<SortItem> sortItems = plan.getSortItems()
                .stream()
                .map(si ->
                {
                    IExpression expression = si.getExpression();
                    if (expressionRewriter != null)
                    {
                        expression = expression.accept(expressionRewriter, context);
                    }
                    else
                    {
                        expression = visit(plan, expression, context);
                    }
                    return new SortItem(expression, si.getOrder(), si.getNullOrder(), si.getLocation());
                })
                .collect(toList());

        return new Sort(plan.getInput()
                .accept(this, context), sortItems);
    }

    protected ILogicalPlan create(Filter plan, C context)
    {
        IExpression predicate = plan.getPredicate();
        if (expressionRewriter != null)
        {
            predicate = predicate.accept(expressionRewriter, context);
        }
        else
        {
            predicate = visit(plan, predicate, context);
        }
        return new Filter(plan.getInput()
                .accept(this, context), plan.getTableSource(), predicate);
    }

    protected ILogicalPlan create(SubQuery plan, C context)
    {
        return new SubQuery(plan.getInput()
                .accept(this, context), plan.getTableSource(), plan.getLocation());
    }

    protected ILogicalPlan create(Limit plan, C context)
    {
        IExpression limitExpression = plan.getLimitExpression();
        if (expressionRewriter != null)
        {
            limitExpression = limitExpression.accept(expressionRewriter, context);
        }
        else
        {
            limitExpression = visit(plan, limitExpression, context);
        }
        return new Limit(plan.getInput()
                .accept(this, context), limitExpression);
    }

    protected ILogicalPlan create(OperatorFunctionScan plan, C context)
    {
        return new OperatorFunctionScan(plan.getSchema(), plan.getInput()
                .accept(this, context), plan.getCatalogAlias(), plan.getFunction(), plan.getLocation());
    }

    protected ILogicalPlan create(ConstantScan plan, C context)
    {
        return plan;
    }

    protected ILogicalPlan create(MaxRowCountAssert plan, C context)
    {
        return new MaxRowCountAssert(plan.getInput()
                .accept(this, context), plan.getMaxRowCount());
    }

    protected ILogicalPlan create(Concatenation plan, C context)
    {
        return new Concatenation(plan.getChildren()
                .stream()
                .map(p -> p.accept(this, context))
                .collect(toList()));
    }

    /** Extract columns from provided expressions */
    protected static Map<TableSourceReference, Set<String>> collectColumns(List<IExpression> expressions)
    {
        Map<TableSourceReference, Set<String>> columns = new HashMap<>();
        for (IExpression expression : expressions)
        {
            expression.accept(ColumnReferenceExtractor.INSTANCE, columns);
        }
        return columns;
    }

    /** Visitor that collects columns from an expression */
    static class ColumnReferenceExtractor extends AExpressionVisitor<Void, Map<TableSourceReference, Set<String>>>
    {
        static final ColumnReferenceExtractor INSTANCE = new ColumnReferenceExtractor();

        @Override
        public Void visit(UnresolvedColumnExpression expression, Map<TableSourceReference, Set<String>> columns)
        {
            throw new IllegalArgumentException("Unresolved column expression should not be visited");
        }

        @Override
        public Void visit(IColumnExpression expression, Map<TableSourceReference, Set<String>> context)
        {
            ColumnExpression ce = (ColumnExpression) expression;
            TableSourceReference tableRef = ce.getColumnReference()
                    .tableSourceReference();
            if (tableRef != null)
            {
                context.computeIfAbsent(tableRef, k -> new HashSet<>())
                        .add(ce.getAlias()
                                .getAlias());
            }
            return null;
        }
    }
}
