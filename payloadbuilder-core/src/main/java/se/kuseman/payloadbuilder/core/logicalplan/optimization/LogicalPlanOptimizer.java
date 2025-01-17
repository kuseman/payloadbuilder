package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;

import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.optimization.ALogicalPlanOptimizer.Context;
import se.kuseman.payloadbuilder.core.logicalplan.optimization.ColumnResolver.ResolveSchema;

/** Plan optimizer that applies rules to a {@link ILogicalPlan} to optimize operators etc. */
public class LogicalPlanOptimizer
{
    private static final SchemaResolver SCHEMA_RESOLVER = new SchemaResolver();
    private static final ColumnResolver COLUMN_RESOLVER = new ColumnResolver();

    //@formatter:off
    private static final List<ALogicalPlanOptimizer<? extends ALogicalPlanOptimizer.Context>> RULES = asList(
            /* First all schemas/types must be resolved
             Eliminate all UnresolvedFunctionCallExpression's */
            SCHEMA_RESOLVER,
            /* Push down all computed expressions into a separate plan above  (to be able to sort on those for example) */
            new ComputedExpressionPushDown(),
            /* Push down all sub query expressions in projection to operators
             * Eliminate all UnresolvedSubqueryExpression's */
            new SubQueryExpressionPushDown(),
            /* Resolve all column references etc. Eliminate all UnresolvedColumnExpression's */
            COLUMN_RESOLVER,
            /* Push down predicates to table sources */
            new PredicatePushDown(),
            /* Collect all used columns and push down to table sources.
             * TODO: Fix this, currently broken due to asterisk selects isn't handled and we get wrong projections */
            //new ProjectionPushDown()
            /* Rule that corrects wrong column ordinals after predicate push down might have changed */
            new ColumnOrdinalResolver()
            );
    //@formatter:on

    private LogicalPlanOptimizer()
    {
    }

    /** Optimize provided plan */
    public static ILogicalPlan optimize(IExecutionContext context, ILogicalPlan plan, Map<QualifiedName, TableSchema> schemaByTempTable, Map<TableSourceReference, TableSchema> schemaByTableSource)
    {
        return optimizeInternal(context, plan, schemaByTempTable, schemaByTableSource, null);
    }

    /** Optimize provided plan but stop before a certain rule class. */
    static ILogicalPlan optimizeInternal(IExecutionContext context, ILogicalPlan plan, Map<QualifiedName, TableSchema> schemaByTempTable, Map<TableSourceReference, TableSchema> schemaByTableSource,
            Class<? extends ALogicalPlanOptimizer<?>> stopRule)
    {
        QuerySession s = (QuerySession) context.getSession();

        ValueVector debugPlanProperty = s.getSystemProperty(QuerySession.DEBUG_PLAN);
        boolean debugPlan = !debugPlanProperty.isNull(0)
                && debugPlanProperty.getBoolean(0);

        if (debugPlan)
        {
            s.printLine("Plan before: " + System.lineSeparator());
            s.printLine(plan.print(0));
        }

        ILogicalPlan result = plan;
        int expressionCounter = 0;
        for (ALogicalPlanOptimizer<? extends ALogicalPlanOptimizer.Context> rule : RULES)
        {
            if (stopRule != null
                    && stopRule.isAssignableFrom(rule.getClass()))
            {
                break;
            }

            Context ctx = rule.createContext(context);
            ctx.expressionCounter = expressionCounter;
            ctx.schemaByTempTable = schemaByTempTable;
            ctx.schemaByTableSource = schemaByTableSource;

            result = rule.optimize(ctx, result);

            if (debugPlan)
            {
                s.printLine("Plan after " + rule.getClass()
                        .getSimpleName() + ": " + System.lineSeparator());
                s.printLine(result.print(0));
            }

            expressionCounter = ctx.expressionCounter;
        }

        return result;
    }

    /**
     * Resolved provided expression. Method used when expressions are used outside of plans. Ie. if-statements etc.
     */
    public static IExpression resolveExpression(IExecutionContext context, IExpression expression)
    {
        if (expression == null)
        {
            return null;
        }

        SchemaResolver.Ctx schemaResolverCtx = SCHEMA_RESOLVER.createContext(context);

        // Resolve functions
        IExpression result = expression.accept(SchemaResolver.ExpressionResolver.INSTANCE, schemaResolverCtx);

        ColumnResolver.Ctx columnResolverCtx = COLUMN_RESOLVER.createContext(context);
        columnResolverCtx.schema = new ResolveSchema(Schema.EMPTY);
        // Run it through column resolver to return error for column expressions etc. that isn't allowed
        return ColumnResolver.ColumnResolverVisitor.rewrite(columnResolverCtx, result);
    }
}
