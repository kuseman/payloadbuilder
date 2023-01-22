package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.FunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.OperatorFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.AggregateWrapperExpression;
import se.kuseman.payloadbuilder.core.expression.FunctionCallExpression;
import se.kuseman.payloadbuilder.core.expression.IAggregateExpression;
import se.kuseman.payloadbuilder.core.expression.SubQueryExpression;
import se.kuseman.payloadbuilder.core.expression.UnresolvedFunctionCallExpression;
import se.kuseman.payloadbuilder.core.logicalplan.Aggregate;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.OperatorFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableFunctionScan;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
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
    public ILogicalPlan create(OperatorFunctionScan plan, Ctx context)
    {
        Pair<String, OperatorFunctionInfo> pair = context.getSession()
                .resolveOperatorFunctionInfo(plan.getCatalogAlias(), plan.getFunction());
        if (pair == null)
        {
            throw new ParseException("No operator function found named: " + plan.getFunction(), plan.getToken());
        }
        ILogicalPlan input = plan.getInput()
                .accept(this, context);

        // Create a temporary schema, this will be corrected in SubQueryExpressionPushDown later on
        Schema schema = Schema.of(Column.of("output", pair.getValue()
                .getType()));
        return new OperatorFunctionScan(schema, input, plan.getCatalogAlias(), plan.getFunction(), plan.getToken());
    }

    @Override
    protected ILogicalPlan create(TableScan plan, Ctx context)
    {
        Catalog catalog = context.getSession()
                .getCatalog(plan.getCatalogAlias());

        Schema schema;
        List<Index> indices = emptyList();
        if (plan.isTempTable())
        {
            schema = context.schemaByTempTable.getOrDefault(plan.getTableSource()
                    .getName()
                    .toDotDelimited()
                    .toLowerCase(), Schema.EMPTY);
        }
        else
        {
            TableSchema tableSchema = catalog.getTableSchema(context.getSession(), plan.getCatalogAlias(), plan.getTableSource()
                    .getName());

            schema = tableSchema.getSchema();
            indices = tableSchema.getIndices();
            // No schema returned, create an asterisk
            if (schema.getColumns()
                    .isEmpty())
            {
                ColumnReference ast = new ColumnReference(plan.getTableSource(), plan.getAlias(), ColumnReference.Type.ASTERISK);
                schema = Schema.of(Column.of(ast, ResolvedType.of(Type.Any)));
            }
        }

        // Recreate schema with correct table and column references
        TableSourceReference tableSource = plan.getTableSource();

        schema = recreate(tableSource, schema);

        return new TableScan(new TableSchema(schema, indices), plan.getTableSource(), plan.getProjection(), plan.isTempTable(), plan.getOptions(), plan.getToken());
    }

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
                    .getFirst(), plan.getToken());
        }

        TableFunctionInfo functionInfo = pair.getValue();

        Schema schema = functionInfo.getSchema(plan.getArguments());
        // No schema returned, create an asterisk
        if (schema.getColumns()
                .isEmpty())
        {
            ColumnReference ast = new ColumnReference(plan.getTableSource(), plan.getAlias(), ColumnReference.Type.ASTERISK);
            schema = Schema.of(Column.of(ast, ResolvedType.of(Type.Any)));
        }

        List<? extends se.kuseman.payloadbuilder.api.expression.IExpression> foldedArguments = functionInfo.foldArguments(plan.getArguments());
        List<IExpression> arguments = foldedArguments.stream()
                .map(e -> ((IExpression) e).accept(ExpressionResolver.INSTANCE, context))
                .collect(toList());

        // Recreate schema with correct table and column references
        TableSourceReference tableSource = plan.getTableSource();
        return new TableFunctionScan(plan.getTableSource(), recreate(tableSource, schema), arguments, plan.getOptions(), plan.getToken());
    }

    private Schema recreate(final TableSourceReference tableSource, Schema schema)
    {
        return new Schema(schema.getColumns()
                .stream()
                .map(c ->
                {
                    ResolvedType type = c.getType();
                    // Force all columns to have a column reference
                    ColumnReference colRef = c.getColumnReference();
                    if (colRef == null)
                    {
                        colRef = tableSource.column(c.getName());
                    }
                    else
                    {
                        // If we have a column reference, then switch it's table source
                        // For example if we have a temp table then we need to set a unique table source
                        // for this plan and not copy the original columns reference
                        colRef = new ColumnReference(tableSource, colRef.getName(), colRef.getType());
                    }

                    return Column.of(colRef, type);
                })
                .collect(toList()));
    }

    /** Resolver for expressions. Sub query plans, function calls (scalar, aggregate) */
    static class ExpressionResolver extends ARewriteExpressionVisitor<Ctx>
    {
        static final ExpressionResolver INSTANCE = new ExpressionResolver();

        @Override
        public IExpression visit(SubQueryExpression exp, Ctx context)
        {
            SubQueryExpression expression = exp;

            if (context.insideAggregateFunction)
            {
                throw new ParseException("Cannot aggregate sub query expressions", expression.getToken());
            }

            return new SubQueryExpression(expression.getInput()
                    .accept(context.schemaResolver, context), expression.getToken());
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
            Token token = expression.getToken();

            Pair<String, ScalarFunctionInfo> pair = context.getSession()
                    .resolveScalarFunctionInfo(expression.getCatalogAlias(), expression.getName());
            if (pair == null)
            {
                throw new ParseException("No scalar function found named: " + expression.getName(), token);
            }

            String catalogAlias = pair.getKey();
            ScalarFunctionInfo functionInfo = pair.getValue();

            // Validate
            if (context.insideAggregateFunction
                    && functionInfo.getFunctionType() == FunctionInfo.FunctionType.AGGREGATE)
            {
                throw new ParseException("Cannot aggregate expressions containing aggregate functions", token);
            }
            // Top level function call must be a scalar function that is also an aggregate
            else if (context.resolvingAggregateProjectionExpression
                    && !context.insideAggregateFunction
                    && !functionInfo.getFunctionType()
                            .isAggregate())
            {
                throw new ParseException(functionInfo.getName() + " is not an aggregate function", token);
            }
            else if (functionInfo.arity() >= 0
                    && expression.getChildren()
                            .size() != functionInfo.arity())
            {
                throw new ParseException("Function " + expression.getName()
                                         + " expected "
                                         + functionInfo.arity()
                                         + " argument(s) but got "
                                         + expression.getChildren()
                                                 .size(),
                        token);
            }

            boolean isAggregate = functionInfo.getFunctionType()
                    .isAggregate();
            if (isAggregate)
            {
                context.insideAggregateFunction = true;
            }

            List<? extends se.kuseman.payloadbuilder.api.expression.IExpression> foldedArguments = functionInfo.foldArguments(expression.getArguments());

            // Resolve arguments
            List<IExpression> arguments = foldedArguments.stream()
                    .map(e -> ((IExpression) e).accept(this, context))
                    .collect(toList());

            IExpression result = new FunctionCallExpression(catalogAlias, functionInfo, expression.getAggregateMode(), arguments);

            if (isAggregate)
            {
                context.insideAggregateFunction = false;
            }

            return result;
        }
    }
}
