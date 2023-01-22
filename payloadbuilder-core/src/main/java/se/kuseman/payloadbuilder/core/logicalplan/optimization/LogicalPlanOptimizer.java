package se.kuseman.payloadbuilder.core.logicalplan.optimization;

import static java.util.Arrays.asList;

import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.logicalplan.ILogicalPlan;
import se.kuseman.payloadbuilder.core.logicalplan.optimization.ALogicalPlanOptimizer.Context;

/** Plan optimizer that applies rules to a {@link ILogicalPlan} to optimize operators etc. */
public class LogicalPlanOptimizer
{
    private static final SchemaResolver SCHEMA_RESOLVER = new SchemaResolver();
    private static final ColumnResolver COLUMN_RESOLVER = new ColumnResolver();

    //@formatter:off
    private static final List<ALogicalPlanOptimizer<? extends ALogicalPlanOptimizer.Context>> RULES = asList(
            // First all schemas/types must be resolved
            SCHEMA_RESOLVER,
            // Push down all computed expressions into a separate plan above  (to be able to sort on those for example)
            new ComputedExpressionPushDown(),
            /* Resolve all column references etc. Eliminate sub queries */
            COLUMN_RESOLVER,
            /* Push down all sub query expressions in projection to operators */
            new SubQueryExpressionPushDown(),
            /* Push down predicates to table sources */
            new PredicatePushDown(),
            /* Collect all used columns and push down to table sources */
            new ProjectionPushDown());
    //@formatter:on

    private LogicalPlanOptimizer()
    {
    }

    /** Optimize provided plan */
    public static ILogicalPlan optimize(IExecutionContext context, ILogicalPlan plan, Map<String, Schema> schemaByTempTable)
    {
        ILogicalPlan result = plan;
        for (ALogicalPlanOptimizer<? extends ALogicalPlanOptimizer.Context> rule : RULES)
        {
            // System.out.println(rule.getClass()
            // .getSimpleName());
            // System.out.println(result.print(0));

            Context ctx = rule.createContext(context);
            ctx.schemaByTempTable = schemaByTempTable;

            result = rule.optimize(ctx, result);
        }

        // System.out.println("Final");
        // System.out.println(result.print(0));

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
        columnResolverCtx.schema = Schema.EMPTY;
        // Run it through column resolver to return error for column expressions etc. that isn't allowed
        return ColumnResolver.ColumnResolverVisitor.rewrite(columnResolverCtx, result);
    }
}
