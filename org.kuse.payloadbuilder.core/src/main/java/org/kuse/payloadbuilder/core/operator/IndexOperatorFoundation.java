package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzeResult;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Class containing information needed to build a index operator */
class IndexOperatorFoundation
{
    /** The outer expressions found that matched an index */
    List<Expression> outerValueExpressions;
    /** The inner expressions found that matched an index */
    List<Expression> innerValueExpressions;
    /**
     * The resulting predicate found that matched an index. Ie. parts that cannot be pushed down. This is either a join condition of a where clause
     */
    AnalyzeResult condition;
    /** The resulting pairs that can be pushed down to operators. Ie. parts that's not part of the index that matched */
    List<AnalyzePair> pushDownPairs;
    /** Resulting index predicate that matched the provided predicate */
    IndexPredicate indexPredicate;

    boolean isEqui()
    {
        return outerValueExpressions != null && outerValueExpressions.size() > 0;
    }

    IndexOperatorFoundation(
            String alias,
            List<Index> indices,
            AnalyzeResult analyzeResult)
    {
        int size = analyzeResult.getPairs().size();
        List<AnalyzePair> equiItems = new ArrayList<>(size);
        List<AnalyzePair> leftOverEquiItems = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            AnalyzePair pair = analyzeResult.getPairs().get(i);
            if (pair.isEqui(alias))
            {
                equiItems.add(pair);
            }
            else
            {
                leftOverEquiItems.add(pair);
            }
        }

        if (equiItems.size() == 0)
        {
            Pair<List<AnalyzePair>, AnalyzeResult> pair = analyzeResult.extractPushdownPairs(alias);
            pushDownPairs = pair.getKey();
            condition = pair.getValue();
            return;
        }

        /*
         * Populates value extractors for provided index.
         *
         * Example
         *
         *   Index [club_id, country_id, art_id]
         *   Condition
         *   a.art_id = s.art_id
         *   a.country_id = 0
         *   a.club_id = 10
         *
         * Result:
         *  outerValueExpressions: [10,        0,            s.art_id]
         *  innerValueExpressions: [a.club_id, a.country_id, a.art_id]
         *  indexPairs:
         *    [a.art_id,      s.art_id],
         *    [a.country_id,  0],
         *    [a.club_id,     10]
         *
         *
         *
        */

        SplitResult splitResult = splitEquiPairs(equiItems, alias, indices);
        indexPredicate = splitResult.index != null
            ? new IndexPredicate(splitResult.index, splitResult.indexColumns)
            : null;

        List<AnalyzePair> valueExpressionsPairs = indexPredicate != null
            ? splitResult.indexPairs
            : splitResult.conditionPairs;

        outerValueExpressions = new ArrayList<>(valueExpressionsPairs.size());
        innerValueExpressions = new ArrayList<>(valueExpressionsPairs.size());

        for (AnalyzePair pair : valueExpressionsPairs)
        {
            Pair<Expression, Expression> p = pair.getExpressionPair(alias);
            outerValueExpressions.add(p.getRight());
            innerValueExpressions.add(p.getLeft());
        }

        leftOverEquiItems.addAll(splitResult.pushDownPairs);
        leftOverEquiItems.addAll(splitResult.conditionPairs);

        AnalyzeResult result = new AnalyzeResult(leftOverEquiItems);
        Pair<List<AnalyzePair>, AnalyzeResult> pair = result.extractPushdownPairs(alias);
        pushDownPairs = pair.getKey();
        condition = pair.getValue();
    }

    /** Split provided equi pairs */
    //CSOFF
    private SplitResult splitEquiPairs(List<AnalyzePair> equiPairs, String alias, List<Index> indices)
    //CSON
    {
        // 1. Split equi pairs into two lists: conditionPairs and pushDownPairs

        List<AnalyzePair> conditionPairs = new ArrayList<>();
        List<AnalyzePair> pushDownPairs = new ArrayList<>();
        List<AnalyzePair> indexPairs = new ArrayList<>();

        for (AnalyzePair pair : equiPairs)
        {
            if (pair.isPushdown(alias))
            {
                pushDownPairs.add(pair);
            }
            else
            {
                conditionPairs.add(pair);
            }
        }

        // 2. Try to find a matching index
        //    - Copy conditionPairs to indexPairs
        //    - Move pushDownPairs to indexPairs
        // We need at least one pair from the condition list
        // to be able to use an index

        List<AnalyzePair> tempPushDown = new ArrayList<>();
        List<String> columns = new ArrayList<>();
        for (Index index : indices)
        {
            // Wildcard index that means we simply pick all conditions
            if (index.getColumnsType() == Index.ColumnsType.WILDCARD)
            {
                List<String> conditionColumns = new ArrayList<>(conditionPairs.size());
                for (AnalyzePair pair : conditionPairs)
                {
                    conditionColumns.add(pair.getColumn(alias));
                }
                return new SplitResult(index, conditionColumns, conditionPairs, conditionPairs, pushDownPairs);
            }

            indexPairs.clear();
            tempPushDown.clear();
            tempPushDown.addAll(pushDownPairs);
            columns.clear();
            columns.addAll(index.getColumns());

            int conditionCount = 0;
            columnsLoop: while (columns.size() > 0)
            {
                String column = columns.remove(0);
                for (AnalyzePair pair : conditionPairs)
                {
                    if (column.equalsIgnoreCase(pair.getColumn(alias)))
                    {
                        conditionCount++;
                        indexPairs.add(pair);
                        // Move on to next column
                        continue columnsLoop;
                    }
                }

                // No match on condition, try pushDown
                Iterator<AnalyzePair> it = tempPushDown.iterator();
                while (it.hasNext())
                {
                    AnalyzePair pair = it.next();
                    if (column.equalsIgnoreCase(pair.getColumn(alias)))
                    {
                        indexPairs.add(pair);
                        // Remove since these should not be used as pushdown
                        // because they are covered by the index
                        it.remove();
                        // Move on to next column
                        break;
                    }
                }
            }

            // At least one condition and all columns found => index match
            if (conditionCount > 0 && indexPairs.size() == index.getColumns().size())
            {
                return new SplitResult(index, index.getColumns(), indexPairs, conditionPairs, tempPushDown);
            }
        }

        // No index found
        return new SplitResult(null, emptyList(), emptyList(), conditionPairs, pushDownPairs);
    }

    /**
     * <pre>
     * Class holding information about a splited condition.
     * Divides the condition into 3 pars:
     *  - conditionPairs
     *      These are pairs that has alias on both side of the euqal ie. a.col = b.col
     *  - pushDownPairs
     *      These are pairs that has an alias on one side othe equal ie. b.col = 10
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
     *      pushDownPairs = [ a.country_id = 0, a.club_id = 0, a.art_id = 123 ]
     *
     * Given index: [ art_id, country_id, club_id ]
     *
     *      // Condition pairs are never removed from list
     *      conditionPairs = [ a.art_id = s.art_id ]
     *
     *      // Index pairs have the same index as the index column
     *      indexPairs = [ a.art_id = s.art_id, a.country_id = 0, a.club_id = 0 ]
     *
     *      // Index column pairs can be removed from pushdown list since these filters is covered when utilizing the index and doesn't need to be evaluated
     *      pushDownPairs = [ a.art_id = 123 ]
     *
     * Given index: [ art_id ]
     *
     *      conditionPairs = [ a.art_id = s.art_id ]
     *      indexPairs = [ a.art_id = s.art_id ]
     *      pushDownPairs = [ a.country_id = 0, a.club_id = 0, a.art_id = 123 ]
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
        /** Push down pairs not matching index / condition */
        final List<AnalyzePair> pushDownPairs;

        SplitResult(Index index, List<String> indexColumns, List<AnalyzePair> indexPairs, List<AnalyzePair> conditionPairs, List<AnalyzePair> pushDownPairs)
        {
            this.index = index;
            this.indexColumns = indexColumns;
            this.indexPairs = indexPairs;
            this.conditionPairs = conditionPairs;
            this.pushDownPairs = pushDownPairs;
        }
    }
}
