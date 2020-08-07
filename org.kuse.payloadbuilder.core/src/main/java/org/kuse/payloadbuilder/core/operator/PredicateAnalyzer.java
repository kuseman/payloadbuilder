package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.kuse.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.parser.AExpressionVisitor;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.InExpression;
import org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression;
import org.kuse.payloadbuilder.core.parser.LogicalBinaryExpression;
import org.kuse.payloadbuilder.core.parser.LogicalNotExpression;
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
        Pair<Expression, AnalyzeResult> extractPushdownPredicate(String alias)
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
                if (pair.isPushdown(alias))
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

        /** Returns pairs that are of Comparison EQUAL type and referencing provided alias on one side only */
        public List<AnalyzePair> getEquiPairs(String alias)
        {
            return pairs
                    .stream()
                    .filter(p -> p.isEqui(alias))
                    .collect(toList());
        }

//        /** Return pairs that are of Comparison EQUAL type */
//        public List<AnalyzePair> getEquiPairs()
//        {
//            return getEquiStream().collect(toList());
//        }
//
//        private Stream<AnalyzePair> getEquiStream()
//        {
//            return pairs
//                    .stream()
//                    .filter(pair -> pair.comparisonType == Type.EQUAL);
//        }

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

        AnalyzeItem getLeft()
        {
            return left;
        }

        AnalyzeItem getRight()
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

        /**
         * Returns expression pair for provided alias.
         *
         * @return Returns a Pair with the expression belonging to the alias as left value and opposite as right.
         */
        public Pair<Expression, Expression> getExpressionPair(String alias)
        {
            if (left.isSingleAlias(alias, false, true))
            {
                return Pair.of(left.expression, right.expression);
            }
            else if (right.isSingleAlias(alias, false, true))
            {
                return Pair.of(right.expression, left.expression);
            }

            throw new IllegalArgumentException("No expressions could be found in this pair for alias " + alias);
        }

        /**
         * <pre>
         * Returns the column for provided alias (if any exits).
         * Only single part qualified names are taken into consideration
         * </pre>
         */
        public String getColumn(String alias)
        {
            boolean leftEligable = left.qname != null
                && left.qname.getParts().size() == 1;
            boolean rightEligable = right != null
                && right.qname != null
                && right.qname.getParts().size() == 1;

            if (leftEligable
                && left.aliases.contains(alias))
            {
                return left.qname.getFirst();
            }
            else if (rightEligable
                && right.aliases.contains(alias))
            {
                return right.qname.getFirst();
            }
            else if (leftEligable
                && left.aliases.contains(""))
            {
                return left.qname.getFirst();
            }
            else if (rightEligable
                && right.aliases.contains(""))
            {
                return right.qname.getFirst();
            }

            return null;
        }

        /**
         * Checks if this pair is a push down candidate for provided alias
         *
         * <pre>
         *  A push down pair is a pair only referencing an empty alias
         *  or the provided alias. And one of the sides must have an alias
         *
         *  Ie.
         *  from source s
         *  inner join article a
         *    on  a.art_id = s.art_id
         *    and a.active_flg                  <--- push down for alias 'a'
         *    and internet_flg                  <--- push down for all aliases, and thus true for 'a'
         *    and a.field > :var                <--- push down for a
         * </pre>
         */
        boolean isPushdown(String alias)
        {
            return left.isSingleAlias(alias, true, true)
                && (right == null
                    || right.isSingleAlias(alias, true, true));
        }

        /** Checks if this pair is a EQUI pair for provided alias
         * 
         * <pre>
         *  A EQUI pair is a pair with type EQUAL
         *  and have provided alias referenced on one AND ONLY one side
         *  and that alias is not referenced on the other side.
         *  
         *  Ie
         *  
         *   a.art_id = 10                                ok
         *   10 = a.art_id                                ok
         *   a.art_id = a.id                              not ok
         *   a.art_id = art_id                            not ok
         *   a.art_id = s.art_id                          ok
         *   func(a.art_id) = func(s.art_id + s.id)       ok
         *   a.art_id > 10                                not ok
         *   a.art_id = other_id                          not ok           
         *   
         * </pre>
         */
        boolean isEqui(String alias)
        {
            // Not an equi type
            if (comparisonType != ComparisonExpression.Type.EQUAL)
            {
                return false;
            }

            if (left.isSingleAlias(alias, false, true))
            {
                return !right.aliases.contains(alias)
                    && !right.aliases.contains("");
            }

            return right.isSingleAlias(alias, false, true);
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

                boolean sameAlias = !leftItem.aliases.isEmpty() && Objects.equals(leftItem.aliases, rightItem.aliases);

                if (!sameAlias)
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

                    AnalyzeItem rightItem = new AnalyzeItem(ie, aliases, null);
                    return new AnalyzePair(Type.IN, leftItem, rightItem);
                }
            }

            if (expression instanceof QualifiedReferenceExpression)
            {
                // A single qualified expression in a predicate is a boolean expression
                // Turn this into a comparison expression
                // ie. active_flg = true
                
                return AnalyzePair.of(new ComparisonExpression(ComparisonExpression.Type.EQUAL, expression, LiteralBooleanExpression.TRUE_LITERAL));
            }
            else if (expression instanceof LogicalNotExpression && ((LogicalNotExpression) expression).getExpression() instanceof QualifiedReferenceExpression)
            {
                // A single qualified expression in a predicate is a boolean expression
                // Turn this into a comparison expression
                // ie. not active_flg
                return AnalyzePair.of(new ComparisonExpression(ComparisonExpression.Type.EQUAL, ((LogicalNotExpression) expression).getExpression(), LiteralBooleanExpression.FALSE_LITERAL));
            }
            Set<String> aliases = new HashSet<>();
            QualifiedReferenceVisitor.getAliases(expression, aliases);
            return new AnalyzePair(Type.UNDEFINED, new AnalyzeItem(expression, aliases, null), null);
        }

        private static AnalyzeItem getQualifiedItem(Expression expression)
        {
            if (expression instanceof QualifiedReferenceExpression)
            {
                QualifiedReferenceExpression qre = (QualifiedReferenceExpression) expression;
                List<String> parts = qre.getQname().getParts();
                String alias = parts.size() > 1 ? parts.get(0) : "";
                QualifiedName qname = qre.getQname().extract(parts.size() > 1 ? 1 : 0);
                return new AnalyzeItem(expression, asSet(alias), qname);
            }

            Set<String> aliases = new HashSet<>();
            QualifiedReferenceVisitor.getAliases(expression, aliases);
            return new AnalyzeItem(expression, aliases, null);
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
        /** Aliases (if any) referenced by this item */
        final Set<String> aliases;
        /**
         * Qualified name (if any) referenced by this item If set then {@link #alias} is omitted from qname.
         **/
        private final QualifiedName qname;

        AnalyzeItem(
                Expression expression,
                Set<String> aliases,
                QualifiedName qname)
        {
            this.expression = expression;
            this.aliases = requireNonNull(aliases, "aliases");
            this.qname = qname;
        }

        //        String getAlias()
        //        {
        //            return alias;
        //        }

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

        /**
         * Checks if this item is a single alias. Ie. no alias or empty alias or provided alias
         */
        boolean isSingleAlias(String alias, boolean includeNonAliases, boolean includeEmptyAlias)
        {
            if (includeNonAliases && aliases.isEmpty())
            {
                return true;
            }

            if (aliases.size() == 1)
            {
                return aliases.contains(alias)
                    || (includeEmptyAlias && aliases.contains(""));
            }
            else if (aliases.size() == 2)
            {
                return aliases.contains(alias)
                    && includeEmptyAlias
                    && aliases.contains("");
            }

            return false;
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
