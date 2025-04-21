package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.defaultIfBlank;
import static org.apache.commons.lang3.StringUtils.lowerCase;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo.Arity;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedFunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedSubQueryExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.ConstantScan;
import se.kuseman.payloadbuilder.core.logicalplan.Filter;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.parser.ParseException;

/**
 * Optimizer that resolves schemas from a {@link ILogicalPlan}. This is the first step after parsing where the schema is populated on table sources from catalog implementators.
 */
public class SchemaResolver extends ALogicalPlanOptimizer<SchemaResolver.Ctx>
{
    SchemaResolver()
    {
        super(ExpressionResolver.INSTANCE);
    }

    static class Ctx extends ALogicalPlanOptimizer.Context
    {
        final SchemaResolver schemaResolver;

        /**
         * Flag to indicate that we are resolving aggregate expressions. If this is true and we are entering a function call then we set {@link #insideAggregateFunction} to true and keep resolving
         * arguments.
         */
        boolean resolvingAggregateProjectionExpression;

        /**
         * Flag to indicate that we are inside an aggregate function. That to know that we cannot use nested aggregate functions since those are not allowed. So any function calls used when this flag
         * is true should be a scalar function.
         */
        boolean insideAggregateFunction;

        Ctx(IExecutionContext context, SchemaResolver resolver)
        {
            super(context);
            this.schemaResolver = resolver;
        }
    }

    @Override
    Ctx createContext(IExecutionContext context)
    {
        return new Ctx(context, this);
    }

    @Override
    ILogicalPlan optimize(Context context, ILogicalPlan plan)
    {
        return plan.accept(this, ((Ctx) context));
    }

    @Override
    public ILogicalPlan visit(Aggregate plan, Ctx context)
    {
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        // Loop aggregate expressions
        List<IExpression> aggregateExpressions = plan.getAggregateExpressions()
                .stream()
                .map(e -> e.accept(ExpressionResolver.INSTANCE, context))
                .collect(toList());

        context.resolvingAggregateProjectionExpression = true;

        // Loop projection expressions
        List<IAggregateExpression> projectionExpressions = plan.getProjectionExpressions()
                .stream()
                .map(e -> (IAggregateExpression) e.accept(ExpressionResolver.INSTANCE, context))
                .collect(toList());

        context.resolvingAggregateProjectionExpression = false;
        return new Aggregate(input, aggregateExpressions, projectionExpressions);
    }

    @Override
    protected ILogicalPlan create(Filter plan, Ctx context)
    {
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        // If there is a constant filter then either remove the filter if true
        // or return a constant scan with zero rows if false
        if (plan.getPredicate()
                .isConstant())
        {
            boolean result = plan.getPredicate()
                    .eval(null)
                    .getPredicateBoolean(0);
            if (result)
            {
                return input;
            }

            return ConstantScan.create(input.getSchema());
        }

        return super.create(plan, context);
    }

    @Override
    protected ILogicalPlan create(TableScan plan, Ctx context)
    {
        String catalogAlias = lowerCase(defaultIfBlank(plan.getCatalogAlias(), context.getSession()
                .getDefaultCatalogAlias()));

        Catalog catalog = context.getSession()
                .getCatalog(catalogAlias);

        Schema schema;
        List<Index> indices = emptyList();
        if (plan.isTempTable())
        {
            QualifiedName table = plan.getTableSource()
                    .getName()
                    .toLowerCase();

            if (!context.schemaByTempTable.containsKey(table))
            {
                throw new QueryException("No temporary table found with name #" + table);
            }

            TableSchema tableSchema = context.schemaByTempTable.get(table);
            schema = tableSchema.getSchema();
            indices = tableSchema.getIndices();
        }
        else
        {
            TableSchema tableSchema = catalog.getTableSchema(context.getSession(), catalogAlias, plan.getTableSource()
                    .getName(), plan.getOptions());

            schema = tableSchema.getSchema();
            if (schema.getSize() > 0
                    && schema.getColumns()
                            .stream()
                            .anyMatch(c -> SchemaUtils.isAsterisk(c, true)))
            {
                throw new ParseException("Schema for table: " + plan.getTableSource()
                                         + " is invalid. Schema must be either empty (asterisk) or have only regular columns. Check implementation of Catalog: "
                                         + catalog.getName(),
                        plan.getLocation());
            }

            indices = tableSchema.getIndices();
        }

        // No schema returned, create an asterisk
        if (SchemaUtils.isAsterisk(schema, true))
        {
            schema = Schema.of(new CoreColumn(plan.getAlias(), ResolvedType.of(Type.Any), "", false, plan.getTableSource(), CoreColumn.Type.ASTERISK));
        }
        else
        {
            schema = recreate(plan.getTableSource(), schema);
        }

        List<Option> options = plan.getOptions()
                .stream()
                .map(o -> new Option(o.getOption(), ExpressionResolver.INSTANCE.visit(o.getValueExpression(), context)))
                .collect(toList());

        TableSchema tableSchema = new TableSchema(schema, indices);
        context.schemaByTableSource.put(plan.getTableSource(), tableSchema);
        return new TableScan(tableSchema, plan.getTableSource(), plan.getProjection(), plan.isTempTable(), options, plan.getLocation());
    }

    /**
     * This validates that the function exists and has correct arity etc. Is also folds and schema resolves it's arguments.
     */
    @Override
    protected ILogicalPlan create(TableFunctionScan plan, Ctx context)
    {
        Pair<String, TableFunctionInfo> pair = context.getSession()
                .resolveTableFunctionInfo(plan.getCatalogAlias(), plan.getTableSource()
                        .getName()
                        .getFirst());
        if (pair == null)
        {
            throw new ParseException("No table function found named: " + plan.getTableSource()
                    .getName()
                    .getFirst(), plan.getLocation());
        }

        TableFunctionInfo functionInfo = pair.getValue();
        validateArity(functionInfo, plan.getArguments()
                .size(), plan.getLocation());

        List<IExpression> arguments = plan.getArguments()
                .stream()
                .map(e -> e.accept(ExpressionResolver.INSTANCE, context))
                .collect(toList());

        List<Option> options = plan.getOptions()
                .stream()
                .map(o -> new Option(o.getOption(), ExpressionResolver.INSTANCE.visit(o.getValueExpression(), context)))
                .collect(toList());

        return new TableFunctionScan(plan.getTableSource(), plan.getSchema(), arguments, options, plan.getLocation());
    }

    static Schema recreate(final TableSourceReference tableSource, Schema schema)
    {
        return new Schema(schema.getColumns()
                .stream()
                .map(c ->
                {
                    ResolvedType type = c.getType();
                    boolean asterisk = SchemaUtils.isAsterisk(c, true);
                    // // Force all columns to have a column reference
                    return new CoreColumn(c.getName(), type, "", false, tableSource, asterisk ? CoreColumn.Type.NAMED_ASTERISK
                            : CoreColumn.Type.REGULAR);
                })
                .collect(toList()));
    }

    /** Resolver for expressions. Sub query plans, function calls (scalar, aggregate) */
    static class ExpressionResolver extends ARewriteExpressionVisitor<Ctx>
    {
        static final ExpressionResolver INSTANCE = new ExpressionResolver();

        @Override
        public IExpression visit(UnresolvedSubQueryExpression exp, Ctx context)
        {
            UnresolvedSubQueryExpression expression = exp;

            if (context.insideAggregateFunction)
            {
                throw new ParseException("Cannot aggregate sub query expressions", expression.getLocation());
            }

            return new UnresolvedSubQueryExpression(expression.getInput()
                    .accept(context.schemaResolver, context), expression.getLocation());
        }

        @Override
        public IExpression visit(AggregateWrapperExpression expression, Ctx context)
        {
            return new AggregateWrapperExpression(expression.getExpression()
                    .accept(this, context), expression.isSingleValue(), expression.isInternal());
        }

        @Override
        public IExpression visit(UnresolvedFunctionCallExpression expression, Ctx context)
        {
            Location location = expression.getLocation();

            Pair<String, ScalarFunctionInfo> pair = context.getSession()
                    .resolveScalarFunctionInfo(expression.getCatalogAlias(), expression.getName());
            if (pair == null)
            {
                throw new ParseException("No scalar function found named: " + expression.getName(), location);
            }

            ScalarFunctionInfo functionInfo = pair.getValue();
            validateArity(functionInfo, expression.getChildren()
                    .size(), location);

            // Validate
            if (context.insideAggregateFunction
                    && functionInfo.getFunctionType() == FunctionInfo.FunctionType.AGGREGATE)
            {
                throw new ParseException("Cannot aggregate expressions containing aggregate functions", location);
            }
            // Top level function call must be a scalar function that is also an aggregate
            else if (context.resolvingAggregateProjectionExpression
                    && !context.insideAggregateFunction
                    && !functionInfo.getFunctionType()
                            .isAggregate())
            {
                throw new ParseException(functionInfo.getName() + " is not an aggregate function", location);
            }

            boolean isAggregate = functionInfo.getFunctionType()
                    .isAggregate();
            if (isAggregate)
            {
                context.insideAggregateFunction = true;
            }

            // Resolve arguments
            List<IExpression> arguments = expression.getArguments()
                    .stream()
                    .map(e -> e.accept(this, context))
                    .collect(toList());

            IExpression result;

            IExpression foldedExpression = functionInfo.fold(context.context, arguments);
            if (foldedExpression != null)
            {
                result = foldedExpression;
            }
            else
            {
                String catalogAlias = pair.getKey();
                result = new FunctionCallExpression(catalogAlias, functionInfo, expression.getAggregateMode(), arguments);
            }

            if (isAggregate)
            {
                context.insideAggregateFunction = false;
            }

            return result;
        }
    }

    private static void validateArity(FunctionInfo functionInfo, int argumentCount, Location location)
    {
        Arity arity = functionInfo.arity();
        if (!arity.satisfies(argumentCount))
        {
            String arityDescription;
            int min = arity.getMin();
            int max = arity.getMax();

            if (min == max)
            {
                arityDescription = "" + min;
            }
            else if (max < 0)
            {
                arityDescription = "at least " + min;
            }
            else
            {
                arityDescription = "between " + min + " and " + max;
            }
            throw new ParseException("Function " + functionInfo.getName() + " expected " + arityDescription + " argument(s) but got " + argumentCount, location);
        }
    }
}
