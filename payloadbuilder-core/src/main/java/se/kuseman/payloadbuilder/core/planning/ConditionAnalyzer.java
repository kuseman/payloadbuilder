package se.kuseman.payloadbuilder.core.planning;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;

import java.util.ArrayList;
import java.util.EnumSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.IntStream;

import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.IPredicate.Type;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Index.IndexType;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzePair;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzeResult;
import se.kuseman.payloadbuilder.core.logicalplan.TableScan;
import se.kuseman.payloadbuilder.core.planning.QueryPlanner.PredicatePair;
import se.kuseman.payloadbuilder.core.planning.SeekPredicate.SeekPredicateItem;

/**
 * Analyzer that analyses a join condition to see which type of join operator that can be used. Also finds available indices to apply
 */
class ConditionAnalyzer
{
    /** Valid index types to use for In expressions */
    private static final Set<ColumnsType> IN_EXPRESSION_PUSH_DOWN_COLUMNS_TYPES = EnumSet.of(ColumnsType.WILDCARD, ColumnsType.ANY, ColumnsType.ANY_IN_ORDER);

    private ConditionAnalyzer()
    {
    }

    private static final Result EMPTY = new Result(List.of(), List.of(), null);

    /** Result of analysis */
    // CSOFF
    //@formatter:off
    record Result(
            /** The outer expressions found that matched an index and/or is part of equi pair */
            List<IExpression> outerValueExpressions,
            /** The inner expressions found that matched an index and/or is part of equi pair */
            List<IExpression> innerValueExpressions,
            /** Resulting seek predicate that matched one index */
            SeekPredicate seekPredicate)
    //@formatter:on
    // CSON
    {
    }

    /** Analyzes predicate pairs and tries to find a suitable index in provided table scan to use */
    static SeekPredicate getSeekPredicate(TableScan scan, List<IPredicate> predicates)
    {
        TableSourceReference tableSource = scan.getTableSource();
        List<Index> indices = scan.getTableSchema()
                .getIndices();
        if (indices.isEmpty())
        {
            return null;
        }

        List<AnalyzePair> equiPairs = new ArrayList<>();
        List<AnalyzePair> inPairs = new ArrayList<>();
        for (IPredicate p : predicates)
        {
            PredicatePair pp = (PredicatePair) p;
            AnalyzePair pair = pp.getAnalyzePair();
            if (pair.getType() == Type.COMPARISION
                    && pair.getComparisonType() == IComparisonExpression.Type.EQUAL)
            {
                equiPairs.add(pair);
            }
            else if (pair.getType() == Type.IN)
            {
                Pair<IExpression, IExpression> ine = pair.getExpressionPair(tableSource);
                // Cannot be NOT
                if (!((IInExpression) ine.getValue()).isNot())
                {
                    inPairs.add(pair);
                }
            }
        }

        // First try equi pairs
        SplitResult splitResult = splitEquiPairs(equiPairs, tableSource, indices);
        if (splitResult.index != null)
        {
            // Remove the predicates we used as index source
            // This should not be used as pushed down later on
            predicates.removeIf(p -> splitResult.indexPairs.contains(((PredicatePair) p).getAnalyzePair()));

            int size = splitResult.indexPairs.size();
            List<IExpression> outerValueExpressions = new ArrayList<>(size);
            List<IExpression> innerValueExpressions = new ArrayList<>(size);

            for (AnalyzePair pair : splitResult.indexPairs)
            {
                Pair<IExpression, IExpression> p = pair.getExpressionPair(tableSource);
                outerValueExpressions.add(p.getRight());
                innerValueExpressions.add(p.getLeft());
            }

            List<SeekPredicateItem> items = IntStream.range(0, size)
                    .mapToObj(i -> new SeekPredicate.SeekPredicateItem(splitResult.indexColumns.get(i), innerValueExpressions.get(i), List.of(outerValueExpressions.get(i))))
                    .toList();
            return new SeekPredicate(splitResult.index, items, true);
        }

        // Try IN pairs
        for (AnalyzePair pair : inPairs)
        {
            String column = pair.getColumn(tableSource);

            for (Index index : indices)
            {
                if (canUseIndexForInExpressionPushDown(index, column))
                {
                    // Remove the predicate we used as index source
                    // This should not be used as pushed down later on
                    predicates.removeIf(p -> pair == ((PredicatePair) p).getAnalyzePair());

                    IInExpression ine = (IInExpression) pair.getExpressionPair(tableSource)
                            .getValue();
                    List<SeekPredicateItem> items = singletonList(new SeekPredicate.SeekPredicateItem(column, ine.getExpression(), ine.getArguments()));
                    return new SeekPredicate(index, items, true);
                }
            }
        }

        return null;
    }

    private static boolean canUseIndexForInExpressionPushDown(Index index, String pairColumn)
    {
        if (!index.supports(IndexType.SEEK_EQ))
        {
            return false;
        }
        // We can use the index if it's of ALL type and the index only have one column
        if (index.getColumnsType() == ColumnsType.ALL
                && index.getColumns()
                        .size() == 1
                && StringUtils.equalsIgnoreCase(pairColumn, index.getColumns()
                        .get(0)))
        {
            return true;
        }

        return IN_EXPRESSION_PUSH_DOWN_COLUMNS_TYPES.contains(index.getColumnsType())
                && (index.getColumnsType() == ColumnsType.WILDCARD
                        || StringUtils.equalsIgnoreCase(pairColumn, index.getColumns()
                                .get(0)));
    }

    /** Analyze condition */
    static Result analyze(List<TableSourceReference> innerTableSource, IExpression predicate, Map<TableSourceReference, TableSchema> schemaByTableSource)
    {
        AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(predicate);
        TableSourceReference tableSource = null;
        List<AnalyzePair> equiPairs = emptyList();
        for (TableSourceReference ts : innerTableSource)
        {
            equiPairs = analyzeResult.getEquiPairs(ts);
            // Skip all pushdowns
            equiPairs.removeIf(p -> p.isPushdown(ts));
            if (!equiPairs.isEmpty())
            {
                tableSource = ts;
                break;
            }
        }

        // No equis, then we cannot do anything else than a nested loop atm.
        if (equiPairs.isEmpty())
        {
            return EMPTY;
        }

        // See if there is any index present on the inner equi pairs
        List<Index> indices = emptyList();
        for (AnalyzePair pair : equiPairs)
        {
            Pair<IExpression, IExpression> expressionPair = pair.getExpressionPair(tableSource);
            IExpression innerExpression = expressionPair.getLeft();

            if (innerExpression instanceof HasColumnReference tsr)
            {
                TableSourceReference tableRef = tsr.getColumnReference()
                        .tableSourceReference();
                if (tableRef != null)
                {
                    // Pick the first table schema we find, later on we might want
                    // to optimize and try to find the best candidate
                    TableSchema tableSchema = schemaByTableSource.get(tableRef);
                    if (tableSchema != null)
                    {
                        indices = tableSchema.getIndices();
                        break;
                    }
                }
            }
        }

        /*
         * @formatter:off
         * Populates value extractors for provided index.
         *
         * Example
         *
         * Index [club_id, country_id, art_id] 
         * Condition a.art_id = s.art_id AND a.country_id = 0 AND a.club_id = 10
         *
         * Result: 
         *   outerValueExpressions: [10,        0,            s.art_id] 
         *   innerValueExpressions: [a.club_id, a.country_id, a.art_id] 
         *   indexPairs:            [a.art_id, s.art_id], [a.country_id, 0], [a.club_id, 10]
         * @formatter:on
         */

        SplitResult splitResult = splitEquiPairs(equiPairs, tableSource, indices);
        List<AnalyzePair> valueExpressionsPairs = splitResult.index != null ? splitResult.indexPairs
                : splitResult.conditionPairs;
        int size = valueExpressionsPairs.size();

        List<IExpression> outerValueExpressions = new ArrayList<>(size);
        List<IExpression> innerValueExpressions = new ArrayList<>(size);
        SeekPredicate seekPredicate = null;

        for (AnalyzePair pair : valueExpressionsPairs)
        {
            Pair<IExpression, IExpression> p = pair.getExpressionPair(tableSource);
            outerValueExpressions.add(p.getRight());
            innerValueExpressions.add(p.getLeft());
        }

        if (splitResult.index != null)
        {
            List<SeekPredicateItem> items = IntStream.range(0, size)
                    .mapToObj(i -> new SeekPredicate.SeekPredicateItem(splitResult.indexColumns.get(i), innerValueExpressions.get(i), List.of(outerValueExpressions.get(i))))
                    .toList();
            seekPredicate = new SeekPredicate(splitResult.index, items);
        }
        return new Result(outerValueExpressions, innerValueExpressions, seekPredicate);
    }

    /** Split provided equi pairs */
    // CSOFF
    private static SplitResult splitEquiPairs(List<AnalyzePair> equiPairs, TableSourceReference tableSource, List<Index> indices)
    // CSON
    {
        if (equiPairs.isEmpty())
        {
            return new SplitResult(null, emptyList(), emptyList(), equiPairs);
        }

        List<AnalyzePair> indexPairs = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        List<String> indexColumns = new ArrayList<>();
        for (Index index : indices)
        {
            // Only EQ seek indices are supported atm.
            if (!index.supports(IndexType.SEEK_EQ))
            {
                continue;
            }

            // Wildcard index that means we simply pick all conditions
            if (index.getColumnsType() == Index.ColumnsType.WILDCARD)
            {
                List<String> conditionColumns = new ArrayList<>(equiPairs.size());
                for (AnalyzePair pair : equiPairs)
                {
                    conditionColumns.add(pair.getColumn(tableSource));
                }
                return new SplitResult(index, conditionColumns, equiPairs, equiPairs);
            }

            indexPairs.clear();
            columns.clear();
            columns.addAll(index.getColumns());
            indexColumns.clear();

            int conditionCount = 0;
            columnsLoop: while (columns.size() > 0)
            {
                String column = columns.remove(0);

                /*
                 * TODO: if we have a pushdown pair on the same column it will be much more efficient to do a constant join
                 *
                 * Index: [art_id]
                 *
                 * on a.art_id = aa.art_id and a.art_id = 123
                 *
                 * Here we can skip aa.art_id altogether and do a fast nested loop with index access and switch inner/outer operator
                 */
                for (AnalyzePair pair : equiPairs)
                {
                    if (column.equalsIgnoreCase(pair.getColumn(tableSource)))
                    {
                        indexColumns.add(column);
                        conditionCount++;
                        indexPairs.add(pair);
                        // Move on to next column
                        continue columnsLoop;
                    }
                }

                // ANY index type then we are only interested in condition pairs
                // so move on to next column
                if (index.getColumnsType() == ColumnsType.ANY)
                {
                    continue;
                }
                // ANY_IN_ORDER then we break here and use what ever we found
                else if (index.getColumnsType() == ColumnsType.ANY_IN_ORDER)
                {
                    break;
                }
            }

            // At least one condition => index match
            if (conditionCount > 0
                    && (index.getColumnsType() != ColumnsType.ALL
                            || indexColumns.size() == index.getColumns()
                                    .size()))
            {
                return new SplitResult(index, indexColumns, indexPairs, equiPairs);
            }
        }

        // No index found
        return new SplitResult(null, emptyList(), emptyList(), equiPairs);
    }

    /**
     * <pre>
     * Class holding information about a splited condition.
     * Divides the condition into 2 pairs:
     *  - conditionPairs
     *      These are pairs that has alias on both side of the euqal ie. a.col = b.col
     *  - indexPairs
     *      There are pairs that matches the found index columns.
     *      Ie. Index [ art_id, country_id ]
     *      pairs: [a.art_id = s.art_id, 123 = a.country_id]
     *
     * Current alias: a
     * Predicate:
     *
     *          a.art_id = s.art_id
     *      and a.country_id = 0
     *      and a.club_id = 0
     *      and a.art_id = 123
     *
     * Split:
     *      conditionPairs = [ a.art_id = s.art_id ]
     *      indexPairs = []
     *
     * Given index: [ art_id, country_id, club_id ]
     *
     *      // Condition pairs are never removed from list
     *      conditionPairs = [ a.art_id = s.art_id ]
     *
     *      // Index pairs have the same index as the index column
     *      indexPairs = [ a.art_id = s.art_id, a.country_id = 0, a.club_id = 0 ]
     *
     * Given index: [ art_id ]
     *
     *      conditionPairs = [ a.art_id = s.art_id ]
     *      indexPairs = [ a.art_id = s.art_id ]
     * </pre>
     */
    private static class SplitResult
    {
        /** Found index */
        final Index index;
        /** Columns utilized from index */
        final List<String> indexColumns;
        /** Matched index pairs */
        final List<AnalyzePair> indexPairs;
        /** Condition pairs used when joining */
        final List<AnalyzePair> conditionPairs;

        SplitResult(Index index, List<String> indexColumns, List<AnalyzePair> indexPairs, List<AnalyzePair> conditionPairs)
        {
            this.index = index;
            this.indexColumns = indexColumns;
            this.indexPairs = indexPairs;
            this.conditionPairs = conditionPairs;
        }
    }
}
