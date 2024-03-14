package se.kuseman.payloadbuilder.core.planning;

import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Index.IndexType;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.HasColumnReference;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzePair;
import se.kuseman.payloadbuilder.core.expression.PredicateAnalyzer.AnalyzeResult;

/**
 * Analyzer that analyses a join condition to see which type of join operator that can be used. Also finds available indices to apply
 */
class ConditionAnalyzer
{
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

        List<IExpression> outerValueExpressions = new ArrayList<>(valueExpressionsPairs.size());
        List<IExpression> innerValueExpressions = new ArrayList<>(valueExpressionsPairs.size());
        SeekPredicate seekPredicate = null;

        for (AnalyzePair pair : valueExpressionsPairs)
        {
            Pair<IExpression, IExpression> p = pair.getExpressionPair(tableSource);
            outerValueExpressions.add(p.getRight());
            innerValueExpressions.add(p.getLeft());
        }

        if (splitResult.index != null)
        {
            seekPredicate = new SeekPredicate(splitResult.index, IndexType.SEEK_EQ, splitResult.indexColumns, outerValueExpressions);
        }
        return new Result(outerValueExpressions, innerValueExpressions, seekPredicate);
    }

    /** Split provided equi pairs */
    // CSOFF
    private static SplitResult splitEquiPairs(List<AnalyzePair> equiPairs, TableSourceReference tableSource, List<Index> indices)
    // CSON
    {
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
