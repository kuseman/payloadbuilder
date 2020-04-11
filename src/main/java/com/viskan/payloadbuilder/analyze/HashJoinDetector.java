package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.parser.tree.AExpressionVisitor;
import com.viskan.payloadbuilder.parser.tree.ComparisonExpression;
import com.viskan.payloadbuilder.parser.tree.ComparisonExpression.Type;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.LogicalBinaryExpression;
import com.viskan.payloadbuilder.parser.tree.QualifiedReferenceExpression;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang3.tuple.Pair;

import gnu.trove.set.hash.THashSet;

/**
 * Detects if a join condition is suitable for hash join.
 * 
 * <pre>
 *  
 * A hash join is possible if there are one more
 * nested AND expressions and at least on of this is an equal expression
 * with single alias on each side
 * </pre>
 **/
public class HashJoinDetector extends AExpressionVisitor<Void, HashJoinDetector.Context>
{
    private static final HashJoinDetector VISITOR = new HashJoinDetector();

    private HashJoinDetector()
    {
    }

    static class Context
    {
        private final Set<String> aliases = new THashSet<>();
        private final List<Expression> leftExpressions = new ArrayList<>();
        private final List<Expression> rightExpressions = new ArrayList<>();

        private String alias;
    }

    /**
     * Detect hash join
     * 
     * @param condition Join condition to analyze
     * @param alias Right side alias in join
     * @return Pair of left and right side hash expressions
     */
    public static Pair<List<Expression>, List<Expression>> detect(Expression condition, String alias)
    {
        Context context = new Context();
        context.alias = alias;
        condition.accept(VISITOR, context);

        return Pair.of(context.leftExpressions, context.rightExpressions);
    }

    @Override
    public Void visit(LogicalBinaryExpression expression, Context context)
    {
        if (expression.getType() != LogicalBinaryExpression.Type.AND)
        {
            return null;
        }

        expression.getLeft().accept(this, context);
        expression.getRight().accept(this, context);

        return null;
    }

    @Override
    public Void visit(ComparisonExpression expression, Context context)
    {
        if (expression.getType() != Type.EQUAL)
        {
            return null;
        }

        context.aliases.clear();
        expression.getLeft().accept(this, context);
        String left = null;
        if (context.aliases.size() == 1)
        {
            left = context.aliases.iterator().next();
        }

        context.aliases.clear();
        expression.getRight().accept(this, context);
        String right = null;
        if (context.aliases.size() == 1)
        {
            right = context.aliases.iterator().next();
        }

        boolean aliasToLeft = context.alias.equals(left);
        boolean aliasToRight = context.alias.equals(right);

        if (left != null && right != null && (aliasToLeft || aliasToRight))
        {
            if (aliasToLeft)
            {
                context.leftExpressions.add(expression.getLeft());
                context.rightExpressions.add(expression.getRight());
            }
            else
            {
                context.rightExpressions.add(expression.getLeft());
                context.leftExpressions.add(expression.getRight());
            }
        }

        return null;
    }

    @Override
    public Void visit(QualifiedReferenceExpression expression, Context context)
    {
        context.aliases.add(expression.getQname().getAlias());
        return null;
    }
}
