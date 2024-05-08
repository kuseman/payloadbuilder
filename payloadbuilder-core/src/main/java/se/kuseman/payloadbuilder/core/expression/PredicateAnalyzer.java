package se.kuseman.payloadbuilder.core.expression;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptySet;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Objects;
import java.util.Set;

import org.apache.commons.lang3.ObjectUtils;
import org.apache.commons.lang3.builder.ReflectionToStringBuilder;
import org.apache.commons.lang3.builder.ToStringStyle;
import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.ILogicalBinaryExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference.ColumnReference;

/**
 * Analyzes a join predicate
 *
 * <pre>
 * Splits join condition by AND and analyses
 * equal expressions be their table sources and columns
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
 * x.id -&gt; TableSources = (tableB, tableC)
 * a.id -&gt; TableSources = (tableA)
 *
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
    public static AnalyzeResult analyze(IExpression predicate)
    {
        if (predicate == null)
        {
            return AnalyzeResult.EMPTY;
        }

        List<IExpression> queue = new ArrayList<>();
        queue.add(predicate);
        List<AnalyzePair> resultItems = new ArrayList<>();

        while (!queue.isEmpty())
        {
            IExpression e = queue.remove(0);
            if (e instanceof LogicalBinaryExpression)
            {
                LogicalBinaryExpression lbe = (LogicalBinaryExpression) e;
                if (lbe.getLogicalType() == ILogicalBinaryExpression.Type.AND)
                {
                    queue.add(0, lbe.getLeft());
                    queue.add(0, lbe.getRight());
                    continue;
                }
            }
            else if (e instanceof NestedExpression)
            {
                queue.add(0, ((NestedExpression) e).getExpression());
                continue;
            }

            resultItems.add(AnalyzePair.of(e));
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
         * Extracts push down predicate for provided table source.
         */
        public Pair<List<AnalyzePair>, AnalyzeResult> extractPushdownPairs(TableSourceReference tableSource)
        {
            return extractPushdownPairs(tableSource, true);
        }

        /**
         * <pre>
         * Extracts push down predicate for provided alias.
         * &#64;param isNullAllowed True if is null predicate is allowed or not
         * </pre>
         */
        public Pair<List<AnalyzePair>, AnalyzeResult> extractPushdownPairs(TableSourceReference tableSource, boolean isNullAllowed)
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
                if (pair.isPushdown(tableSource)
                        && (isNullAllowed
                                || !isNotNull(pair)))
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
        public IExpression getPredicate()
        {
            return getPredicate(pairs);
        }

        /** Construct a predicate from provided paris */
        public static IExpression getPredicate(List<AnalyzePair> pairs)
        {
            int size = pairs.size();
            IExpression result = null;
            for (int i = size - 1; i >= 0; i--)
            {
                AnalyzePair pair = pairs.get(i);
                if (result == null)
                {
                    result = pair.getPredicate();
                }
                else
                {
                    result = new LogicalBinaryExpression(ILogicalBinaryExpression.Type.AND, result, pair.getPredicate());
                }
            }
            return result;
        }

        /** Returns pairs that are of Comparison EQUAL type and referencing provided table source on one side only */
        public List<AnalyzePair> getEquiPairs(TableSourceReference tableSource)
        {
            return pairs.stream()
                    .map(p -> p)
                    .filter(p -> p.isEqui(tableSource))
                    .collect(toList());
        }

        private boolean isNotNull(AnalyzePair pair)
        {
            if (pair.getType() != IPredicate.Type.NULL)
            {
                return false;
            }
            return !((NullPredicateExpression) pair.right.expression).isNot();
        }

        @Override
        public int hashCode()
        {
            return pairs.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == this)
            {
                return true;
            }
            else if (obj == null)
            {
                return false;
            }
            if (obj instanceof AnalyzeResult that)
            {
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

    //
    /**
     * Analyzed pair. A binary operator with left and right analyzed items
     */
    public static class AnalyzePair
    {
        private final IPredicate.Type type;
        private final IComparisonExpression.Type comparisonType;
        private final AnalyzeItem left;
        private final AnalyzeItem right;

        AnalyzePair(IPredicate.Type type, AnalyzeItem left, AnalyzeItem right)
        {
            this(type, null, left, right);
            if (type == IPredicate.Type.COMPARISION)
            {
                throw new IllegalArgumentException("Must provide a ComparisonExpression type");
            }
        }

        AnalyzePair(IPredicate.Type type, IComparisonExpression.Type comparisonType, AnalyzeItem left, AnalyzeItem right)
        {
            this.type = requireNonNull(type, "type");
            this.comparisonType = comparisonType;
            this.left = requireNonNull(left, "left");
            this.right = right;
        }

        public IExpression getLeftExpression()
        {
            return left.expression;
        }

        public IPredicate.Type getType()
        {
            return type;
        }

        public IComparisonExpression.Type getComparisonType()
        {
            return comparisonType;
        }

        /**
         * Get the corresponding predicate from this pair. Ie combining left and right from it's splitted form
         */
        public IExpression getPredicate()
        {
            if (type == IPredicate.Type.COMPARISION)
            {
                return new ComparisonExpression(comparisonType, left.expression, right.expression);
            }
            else if (type == IPredicate.Type.NULL)
            {
                // Right expression contains the whole null predicate
                return right.expression;
            }
            else if (type == IPredicate.Type.IN)
            {
                // Right expression contains the whole in expression
                return right.expression;
            }
            else if (type == IPredicate.Type.LIKE)
            {
                // Right expression contains the whole like expression
                return right.expression;
            }
            else if (type == IPredicate.Type.FUNCTION_CALL)
            {
                // Left expression contains the function call like expression
                return left.expression;
            }

            // Undefined -> return left expression
            return left.expression;
        }

        /**
         * Returns expression pair for provided table source.
         *
         * @return Returns a Pair with the expression belonging to the table source as left value and opposite as right.
         */
        public Pair<IExpression, IExpression> getExpressionPair(TableSourceReference tableSource)
        {
            // If no table sources exists on either right or left then all input table
            // sources qualifies
            if (left.tableSources.isEmpty()
                    && (right == null
                            || right.tableSources.isEmpty()))
            {
                return Pair.of(left.expression, right != null ? right.expression
                        : null);
            }
            else if (left.isSingleTableSource(tableSource, false))
            {
                return Pair.of(left.expression, right != null ? right.expression
                        : null);
            }
            else if (right != null
                    && right.isSingleTableSource(tableSource, false))
            {
                return Pair.of(right.expression, left.expression);
            }

            throw new IllegalArgumentException("No expressions could be found in this pair for table source " + tableSource.getName());
        }

        /**
         * <pre>
         * Returns the column for provided table source (if any exits).
         * Only single part qualified names are taken into consideration
         * If multiple columns are participating in the expression null is returned
         * </pre>
         */
        public String getColumn(TableSourceReference tableSource)
        {
            boolean leftEligable = left.column != null;
            boolean rightEligable = right != null
                    && right.column != null;

            if (leftEligable
                    && left.tableSources.contains(tableSource))
            {
                return left.column;
            }
            else if (rightEligable
                    && right.tableSources.contains(tableSource))
            {
                return right.column;
            }

            return null;
        }

        public String getSqlRepresentation()
        {
            return getPredicate().toString();
        }

        /**
         * Checks if this pair is a push down candidate for provided table source
         *
         * <pre>
         * A push down pair is a pair only referencing a single table source on one or both sides
         *
         * Ie.
         * select *
         * from source s
         * inner join article a
         *   on a.art_id = s.art_id    <--- no push down since we have 2 table sources involved
         * and a.active_flg            <--- push down for alias 'a'
         * and internet_flg            <--- push down for what ever alias it belongs to
         * and a.field > @var          <--- push down for a
         *
         * </pre>
         */
        public boolean isPushdown(TableSourceReference tableSource)
        {
            boolean presentOnLeft = left.isSingleTableSource(tableSource, false);

            // If table source is present on left side
            // then verify that right side is empty or contains the same table source
            if (presentOnLeft)
            {
                return right == null
                        || right.isSingleTableSource(tableSource, true);
            }

            // Else check that right contains the table source
            // and in that case the left side is empty
            return (right == null
                    || right.isSingleTableSource(tableSource, false))
                    && left.tableSources.isEmpty();
        }

        /**
         * Checks if this pair is a EQUI pair for provided table source
         *
         * <pre>
         * A EQUI pair is a pair with type EQUAL
         * and have provided table source referenced single handedly
         * on either side and is not referenced on the other side.
         *
         * Ie.
         * {@code
         * a.art_id       = 10                    equi
         * 10             = a.art_id              equi
         * a.art_id       = a.id                  not equi
         * a.art_id       = art_id                not equi
         * a.art_id       = s.art_id              equi
         * func(a.art_id) = func(s.art_id + s.id) equi
         * a.art_id       > 10                    not equi
         * a.art_id       = other_id              not equi
         * }
         * </pre>
         */
        public boolean isEqui(TableSourceReference tableSource)
        {
            // Not an equi type
            if (comparisonType != IComparisonExpression.Type.EQUAL)
            {
                return false;
            }

            if (left.isSingleTableSource(tableSource, false))
            {
                return !right.tableSources.contains(tableSource);
            }

            return right.isSingleTableSource(tableSource, false);
        }

        @Override
        public int hashCode()
        {
            // CSOFF
            int hashCode = 17;
            hashCode = hashCode * 37 + left.hashCode();
            hashCode = hashCode * 37 + (right != null ? right.hashCode()
                    : 0);
            return hashCode;
            // CSON
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            else if (obj == this)
            {
                return true;
            }
            else if (obj instanceof AnalyzePair that)
            {
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
        static AnalyzePair of(IExpression expression)
        {
            if (expression instanceof ComparisonExpression)
            {
                ComparisonExpression ce = (ComparisonExpression) expression;

                AnalyzeItem leftItem = getQualifiedItem(ce.getLeft());
                AnalyzeItem rightItem = getQualifiedItem(ce.getRight());

                boolean sameAlias = !leftItem.tableSources.isEmpty()
                        && Objects.equals(leftItem.tableSources, rightItem.tableSources);

                if (!sameAlias)
                {
                    return new AnalyzePair(IPredicate.Type.COMPARISION, ce.getComparisonType(), leftItem, rightItem);
                }
            }
            else if (expression instanceof NullPredicateExpression)
            {
                NullPredicateExpression npe = (NullPredicateExpression) expression;
                AnalyzeItem leftItem = getQualifiedItem(npe.getExpression());
                AnalyzeItem rightItem = new AnalyzeItem(npe, emptySet(), null);
                return new AnalyzePair(IPredicate.Type.NULL, leftItem, rightItem);
            }
            else if (expression instanceof InExpression)
            {
                InExpression ie = (InExpression) expression;
                AnalyzeItem leftItem = getQualifiedItem(ie.getExpression());

                // In expressions can only be used as predicates if we have a table source on the left hand side
                if (!leftItem.tableSources.isEmpty())
                {
                    // Fetch all table sources from arguments
                    Set<TableSourceReference> tableSources = new HashSet<>();
                    for (IExpression arg : ie.getArguments())
                    {
                        ColumnExpressionVisitor.getTableSources(arg, tableSources);
                    }

                    AnalyzeItem rightItem = new AnalyzeItem(ie, tableSources, null);
                    return new AnalyzePair(IPredicate.Type.IN, leftItem, rightItem);
                }
            }
            else if (expression instanceof LikeExpression)
            {
                LikeExpression le = (LikeExpression) expression;
                AnalyzeItem leftItem = getQualifiedItem(le.getExpression());

                // Fetch all table sources for pattern
                Set<TableSourceReference> tableSources = new HashSet<>();
                ColumnExpressionVisitor.getTableSources(le.getPatternExpression(), tableSources);
                AnalyzeItem rightItem = new AnalyzeItem(le, tableSources, null);
                return new AnalyzePair(IPredicate.Type.LIKE, leftItem, rightItem);
            }
            else if (expression instanceof FunctionCallExpression)
            {
                FunctionCallExpression fce = (FunctionCallExpression) expression;
                // Fetch all table sources from arguments
                Set<TableSourceReference> tableSources = new HashSet<>();
                for (IExpression arg : fce.getArguments())
                {
                    ColumnExpressionVisitor.getTableSources(arg, tableSources);
                }

                AnalyzeItem leftItem = new AnalyzeItem(fce, tableSources, null);
                return new AnalyzePair(IPredicate.Type.FUNCTION_CALL, leftItem, null);
            }
            else if (expression instanceof ColumnExpression)
            {
                // A single qualified expression in a predicate is a boolean expression
                // Turn this into a comparison expression
                // ie. active_flg = true

                return AnalyzePair.of(new ComparisonExpression(IComparisonExpression.Type.EQUAL, expression, LiteralBooleanExpression.TRUE));
            }
            else if (expression instanceof LogicalNotExpression)
            {
                LogicalNotExpression lne = (LogicalNotExpression) expression;
                if (lne.getExpression() instanceof ColumnExpression)
                {
                    // NOT a.col => a.col = False
                    return AnalyzePair.of(new ComparisonExpression(IComparisonExpression.Type.EQUAL, ((LogicalNotExpression) expression).getExpression(), LiteralBooleanExpression.FALSE));
                }
            }

            Set<TableSourceReference> tableSources = new HashSet<>();
            ColumnExpressionVisitor.getTableSources(expression, tableSources);
            return new AnalyzePair(IPredicate.Type.UNDEFINED, new AnalyzeItem(expression, tableSources, null), null);
        }

        private static AnalyzeItem getQualifiedItem(IExpression expression)
        {
            Set<TableSourceReference> tableSources = new HashSet<>();
            ColumnExpressionVisitor.getTableSources(expression, tableSources);

            String column = null;
            if (expression instanceof ColumnExpression ce)
            {
                column = ce.getAlias()
                        .getAlias();
            }

            return new AnalyzeItem(expression, tableSources, column);
        }
    }

    //
    /** Analyzed item in a pair */
    public static class AnalyzeItem
    {
        /** Marker table source used to when an ambiguous column is encoutered. This to avoid faulty pushdowns of predicates etc. */
        static final TableSourceReference UNKNOWN_TABLE_SOURCE = new TableSourceReference(-1, TableSourceReference.Type.TABLE, "", QualifiedName.of("unkown"), "#");

        /** Expression representing this item */
        private final IExpression expression;
        /** Table sources (if any) referenced by this item. */
        final Set<TableSourceReference> tableSources;

        /**
         * Column name (if any), is set if this expression is a {@link ColumnExpression}
         **/
        private final String column;

        AnalyzeItem(IExpression expression, Set<TableSourceReference> tableSources, String column)
        {
            this.expression = requireNonNull(expression, "expression");
            this.tableSources = requireNonNull(tableSources, "tableSources");
            this.column = column;
        }

        /**
         * Checks if this item is a single table source. Ie. no table source or no tables sources
         */
        boolean isSingleTableSource(TableSourceReference tableSource, boolean includeEmptyTableSources)
        {
            // No table source references counts as single
            return (includeEmptyTableSources
                    && tableSources.isEmpty())
                    || (tableSources.size() == 1
                            && tableSources.contains(tableSource));
        }

        @Override
        public int hashCode()
        {
            return expression != null ? expression.hashCode()
                    : 0;
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj == null)
            {
                return false;
            }
            else if (obj == this)
            {
                return true;
            }
            else if (obj instanceof AnalyzeItem that)
            {
                return Objects.equals(expression, that.expression)
                        && Objects.equals(tableSources, that.tableSources)
                        && Objects.equals(column, that.column);
            }

            return false;
        }

        @Override
        public String toString()
        {
            return tableSources + ":" + expression;
        }
    }

    //
    /** Extracts qualified reference aliases for an expression */
    private static class ColumnExpressionVisitor extends AExpressionVisitor<Void, ColumnExpressionVisitor.Context>
    {
        private static final ColumnExpressionVisitor INSTANCE = new ColumnExpressionVisitor();

        private static class Context
        {
            Set<TableSourceReference> result;
        }

        /**
         * Get aliases for provided expression
         */
        static void getTableSources(IExpression expression, Set<TableSourceReference> result)
        {
            Context context = new Context();
            context.result = result;
            expression.accept(INSTANCE, context);
        }

        @Override
        public Void visit(UnresolvedColumnExpression expression, Context context)
        {
            throw new IllegalArgumentException("Unresolved column expressions should not be visited");
        }

        @Override
        public Void visit(IColumnExpression expression, Context context)
        {
            ColumnExpression ce = (ColumnExpression) expression;

            /*
             * @formatter:off
             * Outer references are not a table source per se when analyzing a predicate, they
             * can be seen as a constant.
             * ie. correlated sub query
             *
             * (
             *   select *
             *   from table b
             *   where b.col = a.col  <--- a.col is an outer reference
             *                             and is eligible as both an index source and predicate pushdown source
             *                             and hence we should not collect it's table source which would prevent it from being used
             * ) x
             *
             * @formatter:on
             */
            if (ce.isOuterReference())
            {
                return null;
            }

            TableSourceReference tableSource = AnalyzeItem.UNKNOWN_TABLE_SOURCE;
            if (expression instanceof HasColumnReference htsr)
            {
                ColumnReference cr = htsr.getColumnReference();
                TableSourceReference columnTableSource = cr != null ? cr.tableSourceReference()
                        : null;
                tableSource = ObjectUtils.defaultIfNull(columnTableSource, tableSource);
            }
            context.result.add(tableSource);
            return null;
        }
    }
}
