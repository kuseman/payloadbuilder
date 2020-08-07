package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Stream;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.kuse.payloadbuilder.core.parser.AExpressionVisitor;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression.Type;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.InExpression;
import org.kuse.payloadbuilder.core.parser.LogicalBinaryExpression;
import org.kuse.payloadbuilder.core.parser.NestedExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

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
                //                resultItems.add(new AnalyzePair(
                //                        new AnalyzeItem(e, null, null),
                //                        AnalyzeItem.EMPTY));
                //null, null, e, null, null, null));
                //                continue;
            }

            resultItems.add(AnalyzePair.of(e));
        }

        return new AnalyzeResult(resultItems);
    }

    //    /**
    //     * Constructs an {@link AnalyzePair} from provided expression
    //     *
    //     * @param expression Expression to retrieve item for
    //     */
    //    private static AnalyzePair getPair(Expression expression)
    //    {
    //        Expression left = expression;
    //        Expression right = null;
    //
    //        /* Split up equal expressions, these are sources for equi expression */
    //        if (expression instanceof ComparisonExpression)
    //        {
    //            ComparisonExpression ce = (ComparisonExpression) expression;
    //            if (ce.getType() == Type.EQUAL)
    //            {
    //                left = ce.getLeft();
    //                right = ce.getRight();
    //            }
    //        }
    //        // TODO:
    ////        else if (expression instanceof InExpression)
    ////        {
    ////            InExpression ie = (InExpression) expression;
    ////            left = ie.getExpression();
    ////            right = ie.getArguments().stream().reduce((a, b) -> new LogicalBinaryExpression(LogicalBinaryExpression.Type.OR, a, b)).get();
    ////        }
    //
    //        Set<String> leftAliases = QualifiedReferenceVisitor.getAliases(left);
    //        Set<String> rightAliases = QualifiedReferenceVisitor.getAliases(right);
    //
    //        String leftColumn = null;
    //        String rightColumn = null;
    //
    //        if (left instanceof QualifiedReferenceExpression)
    //        {
    //            QualifiedReferenceExpression qre = (QualifiedReferenceExpression) left;
    //            if (qre.getQname().getParts().size() <= 2)
    //            {
    //                leftColumn = qre.getQname().getLast();
    //            }
    //        }
    //        if (right instanceof QualifiedReferenceExpression)
    //        {
    //            QualifiedReferenceExpression qre = (QualifiedReferenceExpression) right;
    //            if (qre.getQname().getParts().size() <= 2)
    //            {
    //                rightColumn = qre.getQname().getLast();
    //            }
    //        }
    //
    //        return new AnalyzePair(
    //                new AnalyzeItem(left, leftAliases, leftColumn),
    //                new AnalyzeItem(right, rightAliases, rightColumn));
    //    }

    /** Extracts qualified reference aliases for an expression */
    //    private static class QualifiedReferenceVisitor extends AExpressionVisitor<Void, Set<String>>
    //    {
    //        private static final QualifiedReferenceVisitor QR_VISITOR = new QualifiedReferenceVisitor();
    //
    //        /** Get aliases for provided expression */
    //        static Set<String> getAliases(Expression expression)
    //        {
    //            if (expression == null)
    //            {
    //                return emptySet();
    //            }
    //            Set<String> aliases = new HashSet<>();
    //            expression.accept(QR_VISITOR, aliases);
    //            return aliases;
    //        }
    //
    //        @Override
    //        public Void visit(QualifiedReferenceExpression expression, Set<String> aliases)
    //        {
    //            String alias = expression.getQname().getAlias();
    //            if (alias != null)
    //            {
    //                aliases.add(alias/* != null ? alias : ""*/);
    //            }
    //            return null;
    //        }
    //    }

    /** Result of analysis */
    public static class AnalyzeResult
    {
        private static final AnalyzeResult EMPTY = new AnalyzeResult(emptyList());
        private final List<AnalyzePair> pairs;

        public AnalyzeResult(List<AnalyzePair> pairs)
        {
            this.pairs = requireNonNull(pairs);
        }

        public List<AnalyzePair> getPairs()
        {
            return pairs;
        }

        /**
         * <pre>
         * Extracts push down predicate for provided alias.
         * </pre>
         */
        //        public Pair<Expression, AnalyzeResult> extractPushdownPredicate(String alias, boolean includeAliasLess)
        //        {
        //            if (pairs.isEmpty())
        //            {
        //                return Pair.of(null, this);
        //            }
        //
        //            Expression result = null;
        //            int size = pairs.size();
        //            List<AnalyzePair> leftOvers = new ArrayList<>(size);
        //            for (int i = 0; i < size; i++)
        //            {
        //                AnalyzePair pair = pairs.get(i);
        //                if (pair.isSingleAlias(alias) || (includeAliasLess && pair.isSingleAlias("")))
        //                {
        //                    Expression expression = pair.getPredicate();
        //                    result = result == null
        //                        ? expression
        //                        : new LogicalBinaryExpression(LogicalBinaryExpression.Type.AND, result, expression);
        //                }
        //                else
        //                {
        //                    leftOvers.add(pair);
        //                }
        //            }
        //
        //            return Pair.of(result, new AnalyzeResult(leftOvers));
        //        }

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

        /** Returns pairs that are of Comparison EQUAL type and referencing provided alias on one side only */
        public List<AnalyzePair> getEquiPairs(String alias)
        {
            return getEquiStream()
                    .filter(p -> p.isSingleAlias(alias))
                    .collect(toList());
        }

        /** Return pairs that are of Comparison EQUAL type */
        public List<AnalyzePair> getEquiPairs()
        {
            return getEquiStream().collect(toList());
        }

        private Stream<AnalyzePair> getEquiStream()
        {
            return pairs
                    .stream()
                    .filter(pair -> pair.comparisonType == Type.EQUAL);
        }

        /** Return equi items for provided alias */
        //        public List<AnalyzePair> getEquiPairs(String alias, boolean includeAliasLess)
        //        {
        //            return getEquiPairs(alias, includeAliasLess, false);
        //        }
        //
        //        /** Return equi items for provided alias */
        //        public List<AnalyzePair> getEquiPairs(String alias, boolean includeAliasLess, boolean onlySingleAlias)
        //        {
        //            if (pairs.isEmpty())
        //            {
        //                return emptyList();
        //            }
        //            return pairs.stream()
        //                    .filter(pair -> pair.isEqui(alias)
        //                        || (includeAliasLess && pair.isEqui("")))
        //                    .filter(pair -> !onlySingleAlias || pair.isSingleAlias(alias))
        //                    .collect(toList());
        //        }

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
        private final Type type;
        private final ComparisonExpression.Type comparisonType;
        private final AnalyzeItem left;
        private final AnalyzeItem right;

        AnalyzePair(Type type, AnalyzeItem left, AnalyzeItem right)
        {
            this(type, null, left, right);
            if (type == Type.COMPARISION)
            {
                throw new IllegalArgumentException("Must provide a ComparisonExpression type");
            }
        }

        AnalyzePair(Type type, ComparisonExpression.Type comparisonType, AnalyzeItem left, AnalyzeItem right)
        {
            this.type = requireNonNull(type, "type");
            this.comparisonType = comparisonType;
            this.left = requireNonNull(left, "left");
            this.right = right;
        }

        public Type getType()
        {
            return type;
        }

        public ComparisonExpression.Type getComparisonType()
        {
            return comparisonType;
        }

        public AnalyzeItem getLeft()
        {
            return left;
        }

        public AnalyzeItem getRight()
        {
            return right;
        }

        /**
         * Get the corresponding predicate from this pair. Ie combining left and right from it's splitted form
         */
        public Expression getPredicate()
        {
            if (type == Type.COMPARISION)
            {
                return new ComparisonExpression(comparisonType, left.expression, right.expression);
            }
            else if (type == Type.IN)
            {
                // Right expression contains the whole in expression
                return right.expression;
            }

            // Undefined -> return left expression
            return left.expression;
        }

        //        /**
        //         * Returns expression pair for provided alias.
        //         *
        //         * @return Returns a Pair with the expression belonging to the alias as left value and opposite as right.
        //         */
        //        public Pair<Expression, Expression> getExpressionPair(String alias, boolean includeAliasLess)
        //        {
        //            if (left.column != null
        //                && (left.isSingleAlias(alias)
        //                    || (includeAliasLess && left.isSingleAlias(""))))
        //            {
        //                return Pair.of(left.expression, right.expression);
        //            }
        //            else if (right.column != null
        //                && (right.isSingleAlias(alias)
        //                    || (includeAliasLess && right.isSingleAlias(""))))
        //            {
        //                return Pair.of(right.expression, left.expression);
        //            }
        //            throw new IllegalArgumentException("No expressions could be found in this pair for alias " + alias);
        //        }
        //
        /**
         * Returns the column for provided alias (if any exits). Only single part qualified names are allowed
         */
        public String getColumn(String alias)
        {
            boolean leftEligable = left.qname != null
                && left.qname.getParts().size() == 1;
            boolean rightEligable = right != null
                && right.qname != null
                && right.qname.getParts().size() == 1;

            if (leftEligable
                && alias.equals(left.alias))
            {
                return left.qname.getFirst();
            }
            else if (rightEligable
                && alias.equals(right.alias))
            {
                return right.qname.getFirst();
            }
            else if (leftEligable
                && "".equals(left.alias))
            {
                return left.qname.getFirst();
            }
            else if (rightEligable
                && "".equals(right.alias))
            {
                return right.qname.getFirst();
            }

            return null;
        }

        /** Is this pair a single alias reference */
        boolean isSingleAlias(String alias)
        {
            //
            
            return left.isSingleAlias(alias) && (right == null || right.isSingleAlias(alias));
        }

        //        /**
        //         * <pre>
        //         * Return true if this pair is an equi pair matching provided alias.
        //         * An equi pair is a pair where provided alias is not referenced on both sides
        //         * </pre>
        //         **/
        //        boolean isEqui(String alias)
        //        {
        //            if (left.isEmpty() || right.isEmpty())
        //            {
        //                return false;
        //            }
        //
        //            return (left.isSingleAlias(alias) && !right.contains(alias))
        //                || (!left.contains(alias) && right.isSingleAlias(alias));
        //        }

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
                return type == that.type
                    && comparisonType == that.comparisonType
                    && left.equals(that.left)
                    && right.equals(that.right);
            }
            return super.equals(obj);
        }

        @Override
        public String toString()
        {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
        }

        /** Construct a pair from provided expression */
        static AnalyzePair of(Expression expression)
        {
            if (expression instanceof ComparisonExpression)
            {
                ComparisonExpression ce = (ComparisonExpression) expression;

                // TODO: Might add support for more complex comparison expressions
                // eligable for hash join.
                // ie. func(a.art_id) = func2(s.id)

                AnalyzeItem leftItem = getQualifiedItem(ce.getLeft());
                AnalyzeItem rightItem = getQualifiedItem(ce.getRight());

                boolean sameAlias = Objects.equals(leftItem.alias, rightItem.alias);
                
                if (!sameAlias && (leftItem.alias != null || rightItem.alias != null))
                {
                    return new AnalyzePair(Type.COMPARISION, ce.getType(), leftItem, rightItem);
                }
            }
            else if (expression instanceof InExpression)
            {
                InExpression ie = (InExpression) expression;
                if (ie.getExpression() instanceof QualifiedReferenceExpression)
                {
                    AnalyzeItem leftItem = getQualifiedItem(ie.getExpression());

                    // Fetch all aliases from arguments
                    Set<String> aliases = new HashSet<>();
                    for (Expression arg : ie.getArguments())
                    {
                        QualifiedReferenceVisitor.getAliases(arg, aliases);
                    }

                    AnalyzeItem rightItem = new AnalyzeItem(ie, aliases.size() == 1 ? aliases.iterator().next() : null, null);
                    return new AnalyzePair(Type.IN, leftItem, rightItem);
                }
            }

            Set<String> aliases = new HashSet<>();
            QualifiedReferenceVisitor.getAliases(expression, aliases);
            return new AnalyzePair(Type.UNDEFINED, new AnalyzeItem(expression, aliases.size() == 1 ? aliases.iterator().next() : null, null), null);
        }

        private static AnalyzeItem getQualifiedItem(Expression expression)
        {
            if (expression instanceof QualifiedReferenceExpression)
            {
                QualifiedReferenceExpression qre = (QualifiedReferenceExpression) expression;
                List<String> parts = qre.getQname().getParts();
                String alias = parts.size() > 1 ? parts.get(0) : "";
                QualifiedName qname = qre.getQname().extract(parts.size() > 1 ? 1 : 0);
                return new AnalyzeItem(expression, alias, qname);
            }

            return new AnalyzeItem(expression, null, null);
        }

        /** Type of Pair */
        public enum Type
        {
            /** Comparison pair. One of {@link ComparisonExpression.Type} is used */
            COMPARISION,
            /** In pair. {@link InExpression} is used */
            IN,
            /** Undefined type. No analyze could be made for this item. */
            UNDEFINED
        }
    }

    /** Analyzed item in a pair */
    public static class AnalyzeItem
    {
        /** Expression representing this item */
        private final Expression expression;
        /** Alias (if any) referenced by this item */
        private final String alias;
        /**
         * Qualified name (if any) referenced by this item If set then {@link #alias} is omitted from qname.
         **/
        private final QualifiedName qname;

        AnalyzeItem(
                Expression expression,
                String alias,
                QualifiedName qname)
        {
            this.expression = expression;
            this.alias = alias;
            this.qname = qname;
        }
        
        String getAlias()
        {
            return alias;
        }

        /** Returns true if this item is empty */
        //        boolean isEmpty()
        //        {
        //            return expression == null /*|| CollectionUtils.isEmpty(aliases)*/;
        //        }

        /** Returns true if this item contains provided alias */
        //        boolean contains(String alias)
        //        {
        //            return aliases != null && aliases.contains(alias);
        //        }
        //

        //        /** Checks if this item is a single alias. Ie only only alias is referenced in the expression. */
        boolean isSingleAlias(String alias)
        {
            return this.alias == null
                || alias.equals(this.alias)
                || "".equals(this.alias);
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
                    && Objects.equals(alias, that.alias)
                    && Objects.equals(qname, that.qname);
            }

            return false;
        }

        @Override
        public String toString()
        {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
        }
    }

    /** Extracts qualified reference aliases for an expression */
    private static class QualifiedReferenceVisitor extends AExpressionVisitor<Void, Set<String>>
    {
        private static final QualifiedReferenceVisitor QR_VISITOR = new QualifiedReferenceVisitor();

        /**
         * Get aliases for provided expression
         *
         * @param aliases
         */
        static Set<String> getAliases(Expression expression, Set<String> aliases)
        {
            expression.accept(QR_VISITOR, aliases);
            return aliases;
        }

        @Override
        public Void visit(QualifiedReferenceExpression expression, Set<String> aliases)
        {
            String alias = expression.getQname().getAlias();
            aliases.add(alias != null
                ? alias
                : "");
            return null;
        }
    }

}
