package se.kuseman.payloadbuilder.core.planning;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzePair;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzeResult;

/** Class containing information needed to build a indexed join */
class IndexFoundation
{
    /** The outer expressions found that matched an index and/or is part of equi pair */
    List<IExpression> outerValueExpressions = emptyList();
    /** The inner expressions found that matched an index and/or is part of equi pair */
    List<IExpression> innerValueExpressions = emptyList();
    /** Resulting seek predicate that matched one index */
    SeekPredicate seekPredicate;

    boolean isEqui()
    {
        return !outerValueExpressions.isEmpty();
    }

    IndexFoundation(TableSourceReference tableSource, List<Index> indices, IExpression predicate)
    {
        AnalyzeResult analyzeResult = PredicateAnalyzer.analyze(predicate);

        int size = analyzeResult.getPairs()
                .size();
        List<AnalyzePair> equiItems = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            AnalyzePair pair = analyzeResult.getPairs()
                    .get(i);
            if (pair.isEqui(tableSource))
            {
                equiItems.add(pair);
            }
        }

        if (equiItems.size() == 0)
        {
            return;
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

        SplitResult splitResult = splitEquiPairs(equiItems, tableSource, indices);
        List<AnalyzePair> valueExpressionsPairs = splitResult.index != null ? splitResult.indexPairs
                : splitResult.conditionPairs;

        outerValueExpressions = new ArrayList<>(valueExpressionsPairs.size());
        innerValueExpressions = new ArrayList<>(valueExpressionsPairs.size());

        for (AnalyzePair pair : valueExpressionsPairs)
        {
            Pair<IExpression, IExpression> p = pair.getExpressionPair(tableSource);
            outerValueExpressions.add(p.getRight());
            innerValueExpressions.add(p.getLeft());
        }

        seekPredicate = splitResult.index != null ? new SeekPredicate(splitResult.index, splitResult.indexColumns, outerValueExpressions)
                : null;
    }

    /** Split provided equi pairs */
    // CSOFF
    private SplitResult splitEquiPairs(List<AnalyzePair> equiPairs, TableSourceReference tableSource, List<Index> indices)
    // CSON
    {
        List<AnalyzePair> indexPairs = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        List<String> indexColumns = new ArrayList<>();
        for (Index index : indices)
        {
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
            if (conditionCount > 0)
            {
                if (index.getColumnsType() == ColumnsType.ALL
                        && indexColumns.size() == index.getColumns()
                                .size())
                {
                    return new SplitResult(index, indexColumns, indexPairs, equiPairs/* , tempPushDown */);
                }
                else
                {
                    return new SplitResult(index, indexColumns, indexPairs, equiPairs/* , pushDownPairs */);
                }
            }
        }

        // No index found
        return new SplitResult(null, emptyList(), emptyList(), equiPairs/* , pushDownPairs */);
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
