package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.kuse.payloadbuilder.core.utils.CollectionUtils.asSet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair.Type;
import org.kuse.payloadbuilder.core.parser.AExpressionVisitor;
import org.kuse.payloadbuilder.core.parser.ComparisonExpression;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.InExpression;
import org.kuse.payloadbuilder.core.parser.LikeExpression;
import org.kuse.payloadbuilder.core.parser.LiteralBooleanExpression;
import org.kuse.payloadbuilder.core.parser.LogicalBinaryExpression;
import org.kuse.payloadbuilder.core.parser.LogicalNotExpression;
import org.kuse.payloadbuilder.core.parser.NullPredicateExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

/**
 * Analyzes a join predicate
 *
 * <pre>
 * Splits join condition by AND and analyzes
 * equal expressions be their alias and columns
 *
 *
 * from tableA a
 * inner join
 * (
 *   select *
 *   from tableB b
 *   union all
 *   select *
 *   from tableC c
 * ) x
 *   on x.id = a.id
 *   and x.value = 10
 *
 * x.id = a.id
 * Equal comparison
 *
 * x.id -> TableAliases = (b, c)
 * a.id -> TableAliases = (a)
 *
 * Which aliases is referenced in a pair (x, a)
 *
 *
 * x.value = 10
 * Equal comparison
 *
 * x.value -> TableAliases = (b, c)
 * is push down x => true => push down to b and c
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
     * @param predicate Join condition to analyze
     * @return Analyze result of expression
     */
    public static AnalyzeResult analyze(Expression predicate, TableAlias tableAlias)
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
            if (e instanceof LogicalBinaryExpression)
            {
                LogicalBinaryExpression lbe = (LogicalBinaryExpression) e;
                if (lbe.getType() == LogicalBinaryExpression.Type.AND)
                {
                    queue.add(0, lbe.getLeft());
                    queue.add(0, lbe.getRight());
                    continue;
                }
            }

            resultItems.add(AnalyzePair.of(tableAlias, e));
        }

        return new AnalyzeResult(resultItems);
    }

    /** Result of analysis */
    public static class AnalyzeResult
    {
        static final AnalyzeResult EMPTY = new AnalyzeResult(emptyList());
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
        Pair<List<AnalyzePair>, AnalyzeResult> extractPushdownPairs(String alias)
        {
            return extractPushdownPairs(alias, true);
        }

        /**
         * <pre>
         * Extracts push down predicate for provided alias.
         * &#64;param isNullAllowed True if is null predicate is allowed or not
         * </pre>
         */
        Pair<List<AnalyzePair>, AnalyzeResult> extractPushdownPairs(String alias, boolean isNullAllowed)
        {
            if (pairs.isEmpty())
            {
                return Pair.of(emptyList(), this);
            }

            int size = pairs.size();
            List<AnalyzePair> leftovers = new ArrayList<>(size);
            List<AnalyzePair> pushdowns = new ArrayList<>(size);
            for (int i = 0; i < size; i++)
            {
                AnalyzePair pair = pairs.get(i);
                if (pair.isPushdown(alias)
                    && (isNullAllowed
                        || pair.getType() != Type.NULL))
                {
                    pushdowns.add(pair);
                }
                else
                {
                    leftovers.add(pair);
                }
            }

            return Pair.of(pushdowns, new AnalyzeResult(leftovers));
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

    /**
     * Analyzed pair. A binary operator with left and right analyzed items
     */
    public static class AnalyzePair
    {
        private static final Set<String> EMPTY_ALIASES = asSet("");
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
            else if (type == Type.NOT_NULL || type == Type.NULL)
            {
                // Right expression contains the whole null predicate
                return right.expression;
            }
            else if (type == Type.IN)
            {
                // Right expression contains the whole in expression
                return right.expression;
            }
            else if (type == Type.LIKE)
            {
                // Right expression contains the whole like expression
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
         * Return qualified name for provided alias. Returns first match on either left or right side.
         */
        public QualifiedName getQname(String alias)
        {
            if (left.qname != null && left.aliases.contains(alias))
            {
                return left.qname;
            }
            else if (right != null && right.qname != null && right.aliases.contains(alias))
            {
                return right.qname;
            }
            else if (left.qname != null && left.aliases.contains(""))
            {
                return left.qname;
            }
            else if (right != null && right.qname != null && right.aliases.contains(""))
            {
                return right.qname;
            }

            return null;
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
         *
         *
         * Ie. Disallowed sub aliases.
         * A push down cannot be performed on a populating join that filters on a nested alias
         * For alias aa, a1 is a disallowed sub alias
         *
         * from source s
         * inner join
         * (
         *   article_attribute aa
         *   inner join attribute1 a1 with (populate=true)
         *     on a1.attr1_id = aa.attr1_id
         * ) aa with (populate = true)
         *   on aa.art_id = s.art_id
         * where count(aa.a1) > 0               <--- this one cannot be pushed down onto article_attribute table.
         *                                           Since it's referencing a sub alias which won't be loaded yet if pushed down
         * </pre>
         */
        boolean isPushdown(String alias)
        {
            return left.isSingleAlias(alias, true, true)
                && (right == null
                    || right.isSingleAlias(alias, true, true));
        }

        /**
         * Checks if this pair is a EQUI pair for provided alias
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
            //CSOFF
            int hashCode = 17;
            hashCode = hashCode * 37 + left.hashCode();
            hashCode = hashCode * 37 + (right != null ? right.hashCode() : 0);
            return hashCode;
            //CSON
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
                    && Objects.equals(right, that.right);
            }
            return false;
        }

        @Override
        public String toString()
        {
            return ReflectionToStringBuilder.toString(this, ToStringStyle.MULTI_LINE_STYLE);
        }

        /** Construct a pair from provided expression */
        static AnalyzePair of(TableAlias tableAlias, Expression expression)
        {
            if (expression instanceof ComparisonExpression)
            {
                ComparisonExpression ce = (ComparisonExpression) expression;

                AnalyzeItem leftItem = getQualifiedItem(tableAlias, ce.getLeft());
                AnalyzeItem rightItem = getQualifiedItem(tableAlias, ce.getRight());

                boolean sameAlias = !leftItem.aliases.isEmpty() && Objects.equals(leftItem.aliases, rightItem.aliases);

                if (!sameAlias)
                {
                    return new AnalyzePair(Type.COMPARISION, ce.getType(), leftItem, rightItem);
                }
            }
            else if (expression instanceof NullPredicateExpression)
            {
                NullPredicateExpression npe = (NullPredicateExpression) expression;
                if (npe.getExpression() instanceof QualifiedReferenceExpression)
                {
                    AnalyzeItem leftItem = getQualifiedItem(tableAlias, npe.getExpression());
                    AnalyzeItem rightItem = new AnalyzeItem(npe, emptySet(), null);
                    return new AnalyzePair(npe.isNot() ? Type.NOT_NULL : Type.NULL, leftItem, rightItem);
                }
            }
            else if (expression instanceof InExpression)
            {
                InExpression ie = (InExpression) expression;
                if (ie.getExpression() instanceof QualifiedReferenceExpression)
                {
                    AnalyzeItem leftItem = getQualifiedItem(tableAlias, ie.getExpression());

                    // Fetch all aliases from arguments
                    Set<String> aliases = new HashSet<>();
                    for (Expression arg : ie.getArguments())
                    {
                        QualifiedReferenceVisitor.getAliases(tableAlias, arg, aliases);
                    }

                    AnalyzeItem rightItem = new AnalyzeItem(ie, aliases, null);
                    return new AnalyzePair(Type.IN, leftItem, rightItem);
                }
            }
            else if (expression instanceof LikeExpression)
            {
                LikeExpression le = (LikeExpression) expression;
                if (le.getExpression() instanceof QualifiedReferenceExpression)
                {
                    AnalyzeItem leftItem = getQualifiedItem(tableAlias, le.getExpression());

                    // Fetch all aliases pattern
                    Set<String> aliases = new HashSet<>();
                    QualifiedReferenceVisitor.getAliases(tableAlias, le.getPatternExpression(), aliases);
                    AnalyzeItem rightItem = new AnalyzeItem(le, aliases, null);
                    return new AnalyzePair(Type.LIKE, leftItem, rightItem);
                }
            }
            else if (expression instanceof QualifiedReferenceExpression)
            {
                // A single qualified expression in a predicate is a boolean expression
                // Turn this into a comparison expression
                // ie. active_flg = true

                return AnalyzePair.of(tableAlias, new ComparisonExpression(ComparisonExpression.Type.EQUAL, expression, LiteralBooleanExpression.TRUE_LITERAL));
            }
            else if (expression instanceof LogicalNotExpression && ((LogicalNotExpression) expression).getExpression() instanceof QualifiedReferenceExpression)
            {
                // A single qualified expression in a predicate is a boolean expression
                // Turn this into a comparison expression
                // ie. not active_flg
                return AnalyzePair.of(tableAlias,
                        new ComparisonExpression(ComparisonExpression.Type.EQUAL, ((LogicalNotExpression) expression).getExpression(), LiteralBooleanExpression.FALSE_LITERAL));
            }

            Set<String> aliases = new HashSet<>();
            QualifiedReferenceVisitor.getAliases(tableAlias, expression, aliases);
            return new AnalyzePair(Type.UNDEFINED, new AnalyzeItem(expression, aliases, null), null);
        }

        private static AnalyzeItem getQualifiedItem(TableAlias tableAlias, Expression expression)
        {
            if (expression instanceof QualifiedReferenceExpression)
            {
                QualifiedReferenceExpression qre = (QualifiedReferenceExpression) expression;
                QualifiedName qname = qre.getQname();

                Set<String> aliases = EMPTY_ALIASES;
                Pair<String, String> pair = QualifiedReferenceVisitor.getAlias(tableAlias, qname);

                // Gather all qnames
                /*
                 * Alias hierarchy
                 *  a
                 *    - b
                 *
                 * a.col        <-- Ok alias reference
                 * a.b.col      <-- Not ok reference since b is a child
                 * a.c.col      <-- Ok since c is not a child alias
                 *
                 */

                if (pair.getKey() != null)
                {
                    int extractFrom = 1;
                    String alias = pair.getKey();
                    //CSOFF
                    if (pair.getValue() != null)
                    //CSON
                    {
                        extractFrom = 2;
                        alias += "." + pair.getValue();
                    }

                    aliases = asSet(alias);
                    //CSOFF
                    if (qname.getParts().size() > extractFrom)
                    //CSON
                    {
                        qname = qname.extract(extractFrom);
                    }
                    else
                    {
                        qname = null;
                    }
                }

                return new AnalyzeItem(expression, aliases, qname);
            }

            Set<String> aliases = new HashSet<>();
            QualifiedReferenceVisitor.getAliases(tableAlias, expression, aliases);
            return new AnalyzeItem(expression, aliases, null);
        }

        /** Type of Pair */
        public enum Type
        {
            /** Comparison pair. One of {@link ComparisonExpression.Type} is used */
            COMPARISION,
            /** In pair. {@link InExpression} is used */
            IN,

            /** Like pair. {@link LikeExpression} is used */
            LIKE,

            /** Null predicate. */
            NULL,

            /** Not null predicate. */
            NOT_NULL,

            /** Undefined type. No analyze could be made for this item. Full expression is accessed via {@link AnalyzePair#getLeft()} */
            UNDEFINED
        }
    }

    /** Analyzed item in a pair */
    public static class AnalyzeItem
    {
        /** Expression representing this item */
        private final Expression expression;
        /** Aliases (if any) referenced by this item. */
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

        public QualifiedName getQname()
        {
            return qname;
        }

        public Expression getExpression()
        {
            return expression;
        }

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
            return expression != null ? expression.hashCode() : 0;
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
    private static class QualifiedReferenceVisitor extends AExpressionVisitor<Void, QualifiedReferenceVisitor.Context>
    {
        private static final QualifiedReferenceVisitor QR_VISITOR = new QualifiedReferenceVisitor();
        private static final Pair<String, String> EMPTY_PAIR = Pair.of(null, null);

        /** Visitor context */
        private static class Context
        {
            TableAlias tableAlias;
            Set<String> result;
        }

        /**
         * Get aliases for provided expression
         *
         * @param aliases
         */
        static void getAliases(
                TableAlias tableAlias,
                Expression expression,
                Set<String> result)
        {
            Context context = new Context();
            context.tableAlias = tableAlias;
            context.result = result;
            expression.accept(QR_VISITOR, context);
        }

        /** Returns alias for provided qname and sub alias if exists */
        static Pair<String, String> getAlias(TableAlias tableAlias, QualifiedName qname)
        {
            Pair<String, String> result = find(tableAlias, qname);

            TableAlias current = tableAlias.getParent();
            while (current != null && result == EMPTY_PAIR)
            {
                // Try parents
                result = find(current, qname);
                current = current.getParent();
            }

            return result;
        }

        private static Pair<String, String> find(TableAlias tableAlias, QualifiedName qname)
        {
            List<String> parts = qname.getParts();
            String alias;
            String subAlias = null;
            String part = parts.get(0);
            // First part is the current alias
            if (equalsIgnoreCase(tableAlias.getAlias(), part))
            {
                alias = part;
                if (parts.size() > 1)
                {
                    part = parts.get(1);
                    TableAlias temp = tableAlias.getChildAlias(part);
                    //CSOFF
                    if (temp != null)
                    //CSON
                    {
                        return Pair.of(alias, part);
                    }
                }
                // Alias access, not allowed as a predicate, return a non existent alias
                else if (parts.size() == 1)
                {
                    subAlias = alias;
                    alias = "##";
                }
                return Pair.of(alias, null);
            }

            // Try child alias
            TableAlias temp = tableAlias.getChildAlias(parts.get(0));
            if (temp == null)
            {
                // Sibling alias
                temp = tableAlias.getSiblingAlias(parts.get(0));
            }

            if (temp != null)
            {
                alias = parts.get(0);
                if (parts.size() > 1)
                {
                    temp = temp.getChildAlias(parts.get(1));
                    //CSOFF
                    if (temp != null)
                    //CSON
                    {
                        subAlias = alias;
                        alias = "##";
                    }
                }
                // Child alias access and only one part => combine with parent
                // since this alias is not a valid predicate to push down etc.
                else if (parts.size() == 1)
                {
                    subAlias = alias;
                    alias = "##";
                }

                return Pair.of(alias, subAlias);
            }

            return EMPTY_PAIR;
        }

        @Override
        public Void visit(QualifiedReferenceExpression expression, Context context)
        {
            Pair<String, String> pair = getAlias(context.tableAlias, expression.getQname());
            if (pair.getKey() != null)
            {
                // If there was a sub alias match, add a combined alias
                // This to avoid getting a single alias hit of first part when detecting push down etc.
                if (pair.getValue() != null)
                {
                    context.result.add(pair.getKey() + "." + pair.getValue());
                }
                else
                {
                    context.result.add(pair.getKey());
                }
            }
            return null;
        }
    }
}
