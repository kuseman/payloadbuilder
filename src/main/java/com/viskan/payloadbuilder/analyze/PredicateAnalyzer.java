package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.parser.tree.AExpressionVisitor;
import com.viskan.payloadbuilder.parser.tree.ComparisonExpression;
import com.viskan.payloadbuilder.parser.tree.ComparisonExpression.Type;
import com.viskan.payloadbuilder.parser.tree.Expression;
import com.viskan.payloadbuilder.parser.tree.LogicalBinaryExpression;
import com.viskan.payloadbuilder.parser.tree.NestedExpression;
import com.viskan.payloadbuilder.parser.tree.QualifiedReferenceExpression;

import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;

/**
 * Analyzes a join predicate
 *
 * <pre>
 * Splits join condition by AND and analyzes
 * equal expressions be their alias and columns
 * </pre>
 **/
public class PredicateAnalyzer
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
        List<Expression> queue = new ArrayList<>();
        queue.add(predicate);
        List<AnalyzeItem> resultItems = new ArrayList<>();

        while (!queue.isEmpty())
        {
            Expression e = queue.remove(0);
            if (e instanceof NestedExpression)
            {
                queue.add(e = ((NestedExpression) e).getExpression());
                continue;
            }
            if (e instanceof LogicalBinaryExpression)
            {
                LogicalBinaryExpression lbe = (LogicalBinaryExpression) e;
                if (lbe.getType() == LogicalBinaryExpression.Type.AND)
                {
                    queue.add(lbe.getLeft());
                    queue.add(lbe.getRight());
                    continue;
                }
                resultItems.add(new AnalyzeItem(null, null, e, null, null, null));
                continue;
            }

            AnalyzeItem item = getItem(e);
            resultItems.add(item);
        }

        return new AnalyzeResult(predicate, resultItems);
    }

    /**
     * Constructs an {@link AnalyzeItem} from provided expression
     *
     * @param expression Expression to retrive item for
     */
    private static AnalyzeItem getItem(Expression expression)
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

        String leftAlias = null;
        String rightAlias = null;
        String leftColumn = null;
        String rightColumn = null;

        if (leftAliases.size() == 1)
        {
            leftAlias = leftAliases.iterator().next();
            if (left instanceof QualifiedReferenceExpression)
            {
                QualifiedReferenceExpression qre = (QualifiedReferenceExpression) left;
                if (qre.getQname().getParts().size() <= 2)
                {
                    leftColumn = qre.getQname().getLast();
                }
            }
        }
        if (rightAliases.size() == 1)
        {
            rightAlias = rightAliases.iterator().next();
            if (right instanceof QualifiedReferenceExpression)
            {
                QualifiedReferenceExpression qre = (QualifiedReferenceExpression) right;
                if (qre.getQname().getParts().size() <= 2)
                {
                    rightColumn = qre.getQname().getLast();
                }
            }
        }

        return new AnalyzeItem(leftAlias, leftColumn, left, rightAlias, rightColumn, right);
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
        private final List<AnalyzeItem> items;
        private Expression predicate;

        AnalyzeResult(Expression predicate, List<AnalyzeItem> items)
        {
            this.predicate = requireNonNull(predicate);
            this.items = requireNonNull(items);
        }

        /**
         * <pre>
         * Returns pushdown predicate for provided alias if any.
         * NOTE! Modifies this analyze result and removes extracted pushdown predicates from
         * this result.
         * </pre>
         */
        public Expression extractPushdownPredicate(String alias, boolean includeAliasLess)
        {
            Expression result = null;
            Iterator<AnalyzeItem> it = items.iterator();
            boolean modified = false;
            while (it.hasNext())
            {
                AnalyzeItem item = it.next();
                if (item.isSingleAlias(alias) || (includeAliasLess && item.isSingleAlias("")))
                {
                    Expression expression = item.getPredicate();
                    result = result == null
                        ? expression
                        : new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, result, expression);
                    modified = true;
                    it.remove();
                }
            }

            if (modified)
            {
                this.predicate = items
                        .stream()
                        .map(item -> item.getPredicate())
                        .reduce((a, b) -> new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, a, b))
                        .orElse(null);
            }

            return result;
        }

        /** Return the full predicate (source) for this analyze result */
        public Expression getPredicate()
        {
            return predicate;
        }

        /**
         * Returns true if this analyze result suits provided index ie. all index columns are present in predicate
         *
         * @param leftSide Left or right side of condition
         * @param index Index to analyze
         */
        //        boolean matchesIndex(boolean leftSide, Index index)
        //        {
        //            Set<String> predicateColumns = items
        //                    .stream()
        //                    // Filter alias to match index alias
        //                    .map(i -> leftSide ? i.getLeftColumn() : i.getRightColumn())
        //                    .filter(Objects::nonNull)
        //                    .collect(toSet());
        //
        //            return predicateColumns.containsAll(index.getKeys());
        //        }

        /** Return equi items for provided alias */
        public List<AnalyzeItem> getEquiItems(String alias)
        {
            return items.stream().filter(item -> item.isEqui(alias)).collect(toList());
        }
        
        @Override
        public int hashCode()
        {
            return items.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof AnalyzeResult)
            {
                AnalyzeResult that = (AnalyzeResult) obj;
                return predicate.equals(that.predicate)
                    && items.equals(that.items);

            }
            return false;
        }

        @Override
        public String toString()
        {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
        }

    }

    /**
     * Result item of analysis.
     **/
    public static class AnalyzeItem
    {
        /** Left alias if any */
        private final String leftAlias;
        /** Left column if any */
        private final String leftColumn;
        /** Right alias if any */
        private final String rightAlias;
        /** Right column if any */
        private final String rightColumn;
        private final Expression leftExpression;
        private final Expression rightExpression;

        AnalyzeItem(
                String leftAlias,
                String leftColumn,
                Expression leftExpression,
                String rightAlias,
                String rightColumn,
                Expression rightExpression)
        {
            this.leftAlias = leftAlias;
            this.leftColumn = leftColumn;
            this.leftExpression = leftExpression;
            this.rightAlias = rightAlias;
            this.rightColumn = rightColumn;
            this.rightExpression = rightExpression;
        }

        public String getLeftAlias()
        {
            return leftAlias;
        }

        public String getLeftColumn()
        {
            return leftColumn;
        }

        public String getRightAlias()
        {
            return rightAlias;
        }

        public String getRightColumn()
        {
            return rightColumn;
        }

        public Expression getLeftExpression()
        {
            return leftExpression;
        }

        public Expression getRightExpression()
        {
            return rightExpression;
        }

        /** Returns true if this item is a single alias item matching provided alias */
        boolean isSingleAlias(String alias)
        {
            return (Objects.equals(alias, leftAlias) && rightAlias == null)
                || (Objects.equals(alias, rightAlias) && leftAlias == null);
        }
        
        /** Return true if this item is a equi join item matching provided alias */
        boolean isEqui(String alias)
        {
            return (Objects.equals(alias, leftAlias) && rightAlias != null)
                    || (Objects.equals(alias, rightAlias) && leftAlias != null);
        }

        /** Returns the predicate for this item */
        public Expression getPredicate()
        {
            return rightExpression == null
                ? leftExpression
                : new ComparisonExpression(ComparisonExpression.Type.EQUAL, leftExpression, rightExpression);
        }

        @Override
        public int hashCode()
        {
            return 17 +
                37 * (leftExpression != null ? leftExpression.hashCode() : 0) +
                37 * (rightExpression != null ? rightExpression.hashCode() : 0);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof AnalyzeItem)
            {
                AnalyzeItem that = (AnalyzeItem) obj;
                return Objects.equals(leftAlias, that.leftAlias)
                    && Objects.equals(rightAlias, that.rightAlias)
                    && Objects.equals(leftColumn, that.leftColumn)
                    && Objects.equals(rightColumn, that.rightColumn)
                    && Objects.equals(leftExpression, that.leftExpression)
                    && Objects.equals(rightExpression, that.rightExpression);
            }

            return false;
        }

        @Override
        public String toString()
        {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.JSON_STYLE);
        }
    }
}
