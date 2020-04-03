package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.parser.tree.AExpressionVisitor;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.LogicalBinaryExpression;
import com.viskan.payloadbuilder.parser.tree.LogicalBinaryExpression.Type;
import com.viskan.payloadbuilder.parser.tree.NestedExpression;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.QualifiedReferenceExpression;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

/**
 * Predicate visitor that splits predicate for push down
 *
 * <pre>
 *  Example
 *
 *  SELECT  a.art_id
 *  FROM    source s
 *  INNER JOIN article a
 *      ON  a.art_id = s.art_id
 *      AND a.active_flg
 *      AND a.internet_flg
 *
 *   These two:
 *
 *   AND a.active_flg
 *   AND a.internet_flg
 *
 *   Can be pushed down to "article" operator
 *   and is not needed in join predicate
 *
 *   The rules for a split is that the predicate must be one or more
 *   nested AND expression on top level and a single alias is used
 *   on expression.
 * </pre>
 **/
public class PredicateVisitor extends AExpressionVisitor<Void, PredicateVisitor.Context>
{
    private static final PredicateVisitor VISITOR = new PredicateVisitor();

    private PredicateVisitor()
    {
    }

    // Context used during visiting
    static class Context
    {
        String alias;
        Set<String> aliases = new HashSet<>();

        List<Expression> pushDownExpressions;
        List<Expression> nonPushDownExpressions;

        void appendPushDownExpression(Expression expression)
        {
            if (pushDownExpressions == null)
            {
                pushDownExpressions = new ArrayList<>();
            }
            pushDownExpressions.add(expression);
        }

        void appendNonPushDownExpressions(Expression expression)
        {
            if (nonPushDownExpressions == null)
            {
                nonPushDownExpressions = new ArrayList<>();
            }
            nonPushDownExpressions.add(expression);
        }
    }

    /**
     * Analyze expression
     *
     * @return Returns a pair with key as resulting pushdown expression and value as resulting non pushdown
     **/
    public static Pair<Expression, Expression> analyze(Expression e, String alias)
    {
        Context ctx = new Context();
        ctx.alias = alias;
        e.accept(VISITOR, ctx);

        if (ctx.pushDownExpressions == null)
        {
            VISITOR.analyze(ctx, e);

            if (ctx.pushDownExpressions == null)
            {
                return Pair.of(null, e);
            }
        }

        Expression pushDown = ctx.pushDownExpressions.size() > 1
            ? ctx.pushDownExpressions.stream().reduce((a, b) -> new LogicalBinaryExpression(Type.AND, a, b)).get()
            : ctx.pushDownExpressions.get(0);

        Expression nonPushDown = null;
        if (ctx.nonPushDownExpressions != null)
        {
            nonPushDown = ctx.nonPushDownExpressions.size() > 1
                ? ctx.nonPushDownExpressions.stream().reduce((a, b) -> new LogicalBinaryExpression(Type.AND, a, b)).get()
                : ctx.nonPushDownExpressions.get(0);
        }
        return Pair.of(pushDown, nonPushDown);
    }
    
    @Override
    public Void visit(LogicalBinaryExpression expression, Context context)
    {
        if (expression.getType() != Type.AND)
        {
            return null;
        }

        List<LogicalBinaryExpression> queue = new ArrayList<>();
        queue.add(expression);

        while (!queue.isEmpty())
        {
            LogicalBinaryExpression e = queue.remove(0);

            if (isAnd(e.getLeft()))
            {
                queue.add((LogicalBinaryExpression) e.getLeft());
            }
            else
            {
                analyze(context, e.getLeft());
            }

            if (isAnd(e.getRight()))
            {
                queue.add((LogicalBinaryExpression) e.getRight());
            }
            else
            {
                analyze(context, e.getRight());
            }
        }
        return null;
    }

    @Override
    public Void visit(QualifiedReferenceExpression expression, Context context)
    {
        QualifiedName qname = expression.getQname();
        String alias = qname.getAlias();
        context.aliases.add(alias != null ? alias : context.alias);
        return null;
    }

    private boolean isAnd(Expression expresssion)
    {
        return expresssion instanceof LogicalBinaryExpression
            &&
            ((LogicalBinaryExpression) expresssion).getType() == Type.AND;
    }
    
    private void analyze(Context context, Expression expression)
    {
        context.aliases.clear();
        expression.accept(this, context);
        if (context.aliases.size() == 1 && context.aliases.contains(context.alias))
        {
            context.appendPushDownExpression(unwrap(expression));
        }
        else
        {
            context.appendNonPushDownExpressions(unwrap(expression));
        }
    }
    
    private Expression unwrap(Expression expression)
    {
        if (expression instanceof NestedExpression)
        {
            return ((NestedExpression) expression).getExpression();
        }
        return expression;
    }
}
