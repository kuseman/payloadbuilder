package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.AExpressionVisitor;
import com.viskan.payloadbuilder.parser.ComparisonExpression;
import com.viskan.payloadbuilder.parser.ComparisonExpression.Type;
import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.LogicalBinaryExpression;
import com.viskan.payloadbuilder.parser.NestedExpression;
import com.viskan.payloadbuilder.parser.QualifiedReferenceExpression;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;

/**
 * Analyzes a join predicate
 *
 * <pre>
 * Splits join condition by AND and analyzes
 * equal expressions be their alias and columns
 * </pre>
 **/
class PredicateAnalyzer
{
    private PredicateAnalyzer()
    {
    }

    /**
     * Splits expression into items. Used by detection of hash join, merge join push down predicates.
     *
     * @param condition Join condition to analyze
     * @return Analyze result of expression
     */
    public static AnalyzeResult analyze(Expression predicate)
    {
        if (predicate == null)
        {
            return AnalyzeResult.EMPTY;
        }

        List<Expression> queue = new ArrayList<>();
        queue.add(predicate);
        List<AnalyzePair> resultItems = new ArrayList<>();

        while (!queue.isEmpty())
        {
            Expression e = queue.remove(0);
            if (e instanceof NestedExpression)
            {
                queue.add(0, e = ((NestedExpression) e).getExpression());
                continue;
            }
            if (e instanceof LogicalBinaryExpression)
            {
                LogicalBinaryExpression lbe = (LogicalBinaryExpression) e;
                if (lbe.getType() == LogicalBinaryExpression.Type.AND)
                {
                    queue.add(0, lbe.getLeft());
                    queue.add(0, lbe.getRight());
                    continue;
                }
                resultItems.add(new AnalyzePair(
                        new AnalyzeItem(e, null, null),
                        AnalyzeItem.EMPTY));
                //null, null, e, null, null, null));
                continue;
            }

            resultItems.add(getPair(e));
        }

        return new AnalyzeResult(resultItems);
    }

    /**
     * Constructs an {@link AnalyzePair} from provided expression
     *
     * @param expression Expression to retrieve item for
     */
    private static AnalyzePair getPair(Expression expression)
    {
        Expression left = expression;
        Expression right = null;

        /* Split up equal expressions, these are sources for equi expression */
        if (expression instanceof ComparisonExpression)
        {
            ComparisonExpression ce = (ComparisonExpression) expression;
            if (ce.getType() == Type.EQUAL)
            {
                left = ce.getLeft();
                right = ce.getRight();
            }
        }

        Set<String> leftAliases = QualifiedReferenceVisitor.getAliases(left);
        Set<String> rightAliases = QualifiedReferenceVisitor.getAliases(right);

        String leftColumn = null;
        String rightColumn = null;

        if (left instanceof QualifiedReferenceExpression)
        {
            QualifiedReferenceExpression qre = (QualifiedReferenceExpression) left;
            if (qre.getQname().getParts().size() <= 2)
            {
                leftColumn = qre.getQname().getLast();
            }
        }
        if (right instanceof QualifiedReferenceExpression)
        {
            QualifiedReferenceExpression qre = (QualifiedReferenceExpression) right;
            if (qre.getQname().getParts().size() <= 2)
            {
                rightColumn = qre.getQname().getLast();
            }
        }

        return new AnalyzePair(
                new AnalyzeItem(left, leftAliases, leftColumn),
                new AnalyzeItem(right, rightAliases, rightColumn));
    }

    /** Extracts qualified reference aliases for an expression */
    private static class QualifiedReferenceVisitor extends AExpressionVisitor<Void, Set<String>>
    {
        private static final QualifiedReferenceVisitor QR_VISITOR = new QualifiedReferenceVisitor();

        /** Get aliases for provided expression */
        static Set<String> getAliases(Expression expression)
        {
            if (expression == null)
            {
                return emptySet();
            }
            Set<String> aliases = new HashSet<>();
            expression.accept(QR_VISITOR, aliases);
            return aliases;
        }

        @Override
        public Void visit(QualifiedReferenceExpression expression, Set<String> aliases)
        {
            String alias = expression.getQname().getAlias();
            aliases.add(alias != null ? alias : "");
            return null;
        }
    }

    /** Result of analysis */
    public static class AnalyzeResult
    {
        private static final AnalyzeResult EMPTY = new AnalyzeResult(emptyList());
        final List<AnalyzePair> pairs;

        AnalyzeResult(List<AnalyzePair> pairs)
        {
            this.pairs = requireNonNull(pairs);
        }

        /**
         * <pre>
         * Extracts push down predicate for provided alias.
         * </pre>
         */
        public Pair<Expression, AnalyzeResult> extractPushdownPredicate(String alias, boolean includeAliasLess)
        {
            if (pairs.isEmpty())
            {
                return Pair.of(null, this);
            }

            Expression result = null;
            int size = pairs.size();
            List<AnalyzePair> leftOvers = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                AnalyzePair pair = pairs.get(i);
                if (pair.isSingleAlias(alias) || (includeAliasLess && pair.isSingleAlias("")))
                {
                    Expression expression = pair.getPredicate();
                    result = result == null
                        ? expression
                        : new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, result, expression);
                }
                else
                {
                    leftOvers.add(pair);
                }
            }

            return Pair.of(result, new AnalyzeResult(leftOvers));
        }

        /** Return the full predicate (source) for this analyze result */
        public Expression getPredicate()
        {
            int size = pairs.size();
            Expression result = null;
            for (int i = size - 1; i >= 0; i--)
            {
                if (result == null)
                {
                    result = pairs.get(i).getPredicate();
                }
                else
                {
                    result = new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, result, pairs.get(i).getPredicate());
                }
            }
            return result;
        }

        /** Return equi items for provided alias */
        public List<AnalyzePair> getEquiPairs(String alias, boolean includeAliasLess)
        {
            return getEquiPairs(alias, includeAliasLess, false);
        }

        /** Return equi items for provided alias */
        public List<AnalyzePair> getEquiPairs(String alias, boolean includeAliasLess, boolean onlySingleAlias)
        {
            if (pairs.isEmpty())
            {
                return emptyList();
            }
            return pairs.stream()
                    .filter(pair -> pair.isEqui(alias)
                        || (includeAliasLess && pair.isEqui("")))
                    .filter(pair -> !onlySingleAlias || pair.isSingleAlias(alias))
                    .collect(toList());
        }

        @Override
        public int hashCode()
        {
            return pairs.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof AnalyzeResult)
            {
                AnalyzeResult that = (AnalyzeResult) obj;
                return pairs.equals(that.pairs);

            }
            return false;
        }

        @Override
        public String toString()
        {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
        }

    }

    /** Analyzed pair. A binary operator with left and right analyzed items */
    public static class AnalyzePair
    {
        // private final Operator op;
        private final AnalyzeItem left;
        private final AnalyzeItem right;

        AnalyzePair(AnalyzeItem left, AnalyzeItem right)
        {
            this.left = requireNonNull(left, "left");
            this.right = requireNonNull(right, "right");
        }

        public Expression getLeftExpression()
        {
            return left.expression;
        }

        public Expression getRightExpression()
        {
            return right.expression;
        }

        /**
         * Get the corresponding predicate from this pair. Ie combining left and right from it's splitted form
         */
        public Expression getPredicate()
        {
            return right.expression == null
                ? left.expression
                : new ComparisonExpression(ComparisonExpression.Type.EQUAL, left.expression, right.expression);
        }

        /**
         * Returns expression pair for provided alias.
         *
         * @return Returns a Pair with the expression belonging to the alias as left value and opposite as right.
         */
        public Pair<Expression, Expression> getExpressionPair(String alias, boolean includeAliasLess)
        {
            if (left.column != null
                && (left.isSingleAlias(alias)
                    || (includeAliasLess && left.isSingleAlias(""))))
            {
                return Pair.of(left.expression, right.expression);
            }
            else if (right.column != null
                && (right.isSingleAlias(alias)
                    || (includeAliasLess && right.isSingleAlias(""))))
            {
                return Pair.of(right.expression, left.expression);
            }
            throw new IllegalArgumentException("No expressions could be found in this pair for alias " + alias);
        }

        /**
         * Returns the column for provided alias (if any exits)
         *
         * @param includeAliasLess True if alias less columns be returned
         */
        public String getColumn(String alias, boolean includeAliasLess)
        {
            if (left.column != null
                && (left.isSingleAlias(alias)
                    || (includeAliasLess && left.isSingleAlias(""))))
            {
                return left.column;
            }
            else if (right.column != null
                && right.isSingleAlias(alias)
                || (includeAliasLess && right.isSingleAlias("")))
            {
                return right.column;
            }
            return null;
        }

        /** Returns true if this pair is a pushdown for provided alias */
        boolean isPushDown(String alias, boolean includeAliasLess)
        {
            return isSingleAlias(alias) || (includeAliasLess && isSingleAlias(""));
        }

        /** Is this pair a single alias reference */
        boolean isSingleAlias(String alias)
        {
            return left.isSingleAlias(alias) && right.isSingleAlias(alias);
        }

        /**
         * <pre>
         * Return true if this pair is an equi pair matching provided alias.
         * An equi pair is a pair where provided alias is not referenced on both sides
         * </pre>
         **/
        boolean isEqui(String alias)
        {
            if (left.isEmpty() || right.isEmpty())
            {
                return false;
            }

            return (left.isSingleAlias(alias) && !right.contains(alias))
                || (!left.contains(alias) && right.isSingleAlias(alias));
        }

        @Override
        public int hashCode()
        {
            return 17 +
                31 * left.hashCode() +
                37 * right.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof AnalyzePair)
            {
                AnalyzePair that = (AnalyzePair) obj;
                return left.equals(that.left)
                    && right.equals(that.right);
            }
            return super.equals(obj);
        }

        @Override
        public String toString()
        {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
        }
    }

    /** Analyzed item in a pair */
    public static class AnalyzeItem
    {
        private static final AnalyzeItem EMPTY = new AnalyzeItem(null, null, null);

        /** Expression representing this item */
        private final Expression expression;
        /** Set of aliaes contained in this item */
        private final Set<String> aliases;
        /** Column referenced in this item. Only applicable if {@link #expression} is of {@link QualifiedReferenceExpression} */
        private final String column;

        AnalyzeItem(Expression expression,
                Set<String> aliases,
                String column)
        {
            this.expression = expression;
            this.aliases = aliases;
            this.column = column;
        }

        /** Returns true if this item is empty */
        boolean isEmpty()
        {
            return expression == null /*|| CollectionUtils.isEmpty(aliases)*/;
        }

        /** Returns true if this item contains provided alias */
        boolean contains(String alias)
        {
            return aliases != null && aliases.contains(alias);
        }

        /** Checks if this item is a single alias. Ie only only alias is referenced in the expression. */
        boolean isSingleAlias(String alias)
        {
            return aliases != null
                && (aliases.isEmpty()
                    || (aliases.size() == 1 && aliases.contains(alias)));
        }

        @Override
        public int hashCode()
        {
            return 17 +
                37 * (expression != null ? expression.hashCode() : 0);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof AnalyzeItem)
            {
                AnalyzeItem that = (AnalyzeItem) obj;
                return Objects.equals(expression, that.expression)
                    && Objects.equals(aliases, that.aliases)
                    && Objects.equals(column, that.column);
            }

            return false;
        }

        @Override
        public String toString()
        {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
        }
    }
}
