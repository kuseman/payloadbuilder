package se.kuseman.payloadbuilder.core.planning;

import static java.util.stream.Collectors.toList;

import java.util.List;

import se.kuseman.payloadbuilder.api.expression.ICaseExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.expression.ARewriteExpressionVisitor;
import se.kuseman.payloadbuilder.core.expression.CaseExpression;
import se.kuseman.payloadbuilder.core.expression.InstrumentedCaseExpression;
import se.kuseman.payloadbuilder.core.expression.InstrumentedLogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.expression.LogicalBinaryExpression;

/**
 * Expression visitor that replaces {@link CaseExpression} nodes with {@link InstrumentedCaseExpression} and {@link LogicalBinaryExpression} nodes with {@link InstrumentedLogicalBinaryExpression} for
 * branch/condition coverage tracking. Children are visited first so nested expressions are also instrumented.
 */
class CoverageExpressionRewriter extends ARewriteExpressionVisitor<StatementPlanner.Context>
{
    static final CoverageExpressionRewriter INSTANCE = new CoverageExpressionRewriter();

    static IExpression rewrite(StatementPlanner.Context context, IExpression expression)
    {
        if (expression == null)
        {
            return null;
        }
        return expression.accept(INSTANCE, context);
    }

    @Override
    public IExpression visit(ICaseExpression expression, StatementPlanner.Context context)
    {
        // Visit children first to instrument nested CASE expressions
        List<ICaseExpression.WhenClause> whenClauses = expression.getWhenClauses()
                .stream()
                .map(w -> new ICaseExpression.WhenClause(visit(w.getCondition(), context), visit(w.getResult(), context)))
                .collect(toList());
        IExpression elseExpr = expression.getElseExpression() != null ? visit(expression.getElseExpression(), context)
                : null;

        // Preserve location from the original CaseExpression
        CaseExpression rewritten = expression instanceof CaseExpression ce ? new CaseExpression(whenClauses, elseExpr, ce.getLocation())
                : new CaseExpression(whenClauses, elseExpr);

        return new InstrumentedCaseExpression(context.getNextNodeId(), rewritten);
    }

    @Override
    public IExpression visit(ILogicalBinaryExpression expression, StatementPlanner.Context context)
    {
        // Visit children first so nested AND/OR are also instrumented
        IExpression left = visit(expression.getLeft(), context);
        IExpression right = visit(expression.getRight(), context);

        LogicalBinaryExpression rebuilt = new LogicalBinaryExpression(expression.getLogicalType(), left, right);
        if (expression instanceof LogicalBinaryExpression lbe)
        {
            rebuilt.withLocation(lbe.getLocation());
        }

        return new InstrumentedLogicalBinaryExpression(context.getNextNodeId(), rebuilt);
    }
}
