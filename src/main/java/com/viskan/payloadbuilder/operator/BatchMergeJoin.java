package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;

/**
 * <pre>
 * Special variant of merge join that merges two streams by first batching outer rows
 * And push these down to inner operator that in turn returns inner rows connected to the outer rows
 * in outer row order using it's index.
 *
 * Two inputs
 * - left key: id
 * - right key: a_id
 *
 *
 *  a (id)           b (a_id, value)
 *  1                1, 1
 *                   1, 2
 *  2                2, 10
 *  3                3, 1
 *                   3, 2
 *                   3, 3
 *
 * Will create the following tuples:
 *
 * 1 - 1,1
 * 1 - 1,2
 * 2 - 2,10
 * 3 - 3,1
 * 3 - 3,2
 * 3-  3,3
 * 
 * Has 4 modes:
 * 
 * - one to one
 * - one to many
 * - many to one
 * - many to many
 * </pre>
 **/
public class BatchMergeJoin implements Operator
{
    private final String logicalOperator;
    private final Operator outer;
    private final Operator inner;
    private final ValuesExtractor outerValuesExtractor;
    private final ValuesExtractor innerValuesExtractor;
    private final BiPredicate<EvaluationContext, Row> predicate;
    private final RowMerger rowMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;
    private final Index innerIndex;
    private final int valuesSize;
    private final int batchSize;

    /* Statistics */
    private int executionCount;

    public BatchMergeJoin(
            String logicalOperator,
            Operator outer,
            Operator inner,
            ValuesExtractor outerValuesExtractor,
            ValuesExtractor innerValuesExtractor,
            BiPredicate<EvaluationContext, Row> predicate,
            RowMerger rowMerger,
            boolean populating,
            boolean emitEmptyOuterRows,
            Index innerIndex,
            int batchSize)
    {
        this.logicalOperator = logicalOperator;
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.outerValuesExtractor = requireNonNull(outerValuesExtractor, "outerValuesExtractor");
        this.innerValuesExtractor = requireNonNull(innerValuesExtractor, "innerValuesExtractor");
        this.predicate = requireNonNull(predicate, "predicate");
        this.rowMerger = requireNonNull(rowMerger, "rowMerger");
        this.populating = populating;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
        this.innerIndex = requireNonNull(innerIndex, "innerIndex");
        this.valuesSize = innerIndex.getColumns().size();
        this.batchSize = batchSize;
    }
    
    static void clearJoinData(Row row)
    {
        row.hash = 0;
        row.extractedValues = null;
        row.match = false;
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        executionCount++;
        final Iterator<Row> outerIt = outer.open(context);
        return new Iterator<Row>()
        {
            /** Batched rows */
            private List<Row> outerRows;
            private int outerIndex = 0;
            private Iterator<Row> innerIt;

            private Row next;
            /** Current process outer row */
            private Row outerRow;
            /** Current process inner row */
            private Row innerRow;
            
            private final Object[] keyValues = new Object[valuesSize];

            /**
             * <pre>
             * Index pointing to last seen larger than value for when outerRow > innerRow. 
             * Used in many-mode when rewinding outer rows.
             * </pre>
             */
            private int largerThanIndex = 0;
            /** Index of outerRows where to rewind in case of man-mode */
            private int rewindIndex = -1;

            /** Flag used when having populating join or, left join */
            private boolean emitOuterRows;

            @Override
            public Row next()
            {
                Row result = next;
                clearJoinData(result);
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    // Populating mode
                    if (emitOuterRows)
                    {
                        // Complete
                        if (outerIndex > outerRows.size() - 1)
                        {
                            clearOuterRows();
                            continue;
                        }

                        Row row = outerRows.get(outerIndex);
                        // Populating and row is a match of empty outer rows should be emitted
                        // Or non populating and no match
                        if ((populating && (row.match || emitEmptyOuterRows))
                            || (!populating && !row.match))
                        {
                            next = row;
                        }

                        outerIndex++;
                        continue;
                    }

                    if (outerRows == null)
                    {
                        batchOuterRows();
                        if (outerRows.isEmpty())
                        {
                            context.setIndex(null, null);
                            return false;
                        }

                        context.setIndex(innerIndex, outerValuesIterator());
                        innerIt = inner.open(context);
                        if (!innerIt.hasNext())
                        {
                            if (emitEmptyOuterRows)
                            {
                                emitOuterRows = true;
                                continue;
                            }

                            outerRows = null;
                            continue;
                        }
                    }

                    outerIndex = Math.min(outerIndex, outerRows.size() - 1);
                    outerRow = outerRows.get(outerIndex);

                    if (innerRow == null)
                    {
                        innerRow = innerIt.hasNext() ? innerIt.next() : null;
                        if (innerRow == null)
                        {
                            // Populating mode, emit outer rows
                            if (populating)
                            {
                                outerIndex = 0;
                                emitOuterRows = true;
                            }
                            // Inner batch is ended and we haven't process all outer rows yet
                            // process the last ones
                            else if (emitEmptyOuterRows && outerIndex < outerRows.size() - 1)
                            {
                                outerIndex = largerThanIndex;
                                emitOuterRows = true;
                            }
                            else
                            {
                                clearOuterRows();
                            }
                            continue;
                        }
                    }

                    int cResult = compare(context.getEvaluationContext(), outerRow, innerRow);

                    // outerRow > innerRow
                    if (cResult > 0)
                    {
                        // Rewind state
                        if (rewindIndex != -1)
                        {
                            outerRow = null;
                            largerThanIndex = outerIndex;
                            outerIndex = rewindIndex;
                        }
                        innerRow = null;
                        continue;
                    }
                    // outerRow < innerRow
                    else if (cResult < 0)
                    {
                        if (rewindIndex != -1)
                        {
                            // Clear rewind status and move to
                            // the index where outer was last seen as larger than inner
                            outerIndex = largerThanIndex;
                            largerThanIndex = 0;
                            rewindIndex = -1;
                        }
                        else
                        {
                            if (!populating && emitEmptyOuterRows)
                            {
                                next = outerRow;
                            }
                            outerIndex++;
                        }
                        outerRow = null;
                        continue;
                    }

                    innerRow.setPredicateParent(outerRow);
                    if (predicate.test(context.getEvaluationContext(), innerRow))
                    {
                        next = rowMerger.merge(outerRow, innerRow, populating);
                        outerRow.match = true;
                        if (populating)
                        {
                            next = null;
                        }
                    }
                    innerRow.clearPredicateParent();

                    outerIndex++;
                    outerRow = null;

                    // Enter rewind mode on first equal encounter
                    if (rewindIndex == -1)
                    {
                        rewindIndex = outerIndex - 1;
                        largerThanIndex = 0;
                    }
                    // We reached first encountered larger than index,
                    // rewind here instead of trying next outer since we know
                    // it's going to be non equal
                    else if (largerThanIndex > 0 && outerIndex >= largerThanIndex)
                    {
                        innerRow = null;
                        outerIndex = rewindIndex;
                    }

                    // End of outer, clear inner and rewind
                    if (outerIndex > outerRows.size() - 1)
                    {
                        innerRow = null;
                        outerIndex = rewindIndex;
                    }

                }
                return true;
            }
            
            private int compare(EvaluationContext context, Row outerRow, Row innerRow)
            {
                Object[] outerValues = outerRow.extractedValues;
                populateKeyValues(innerValuesExtractor, context, innerRow);
                Object[] innerValues = keyValues;
                
                int length = outerValues.length;
                
                for (int i=0;i<length;i++)
                {
                    @SuppressWarnings("unchecked")
                    Comparable<Object> outer = (Comparable<Object>) outerValues[i];
                    int c = outer.compareTo(innerValues[i]);
                    if (c != 0)
                    {
                        return c;
                    }
                }
                
                return 0;
            }
            
            private void clearOuterRows()
            {
                largerThanIndex = 0;
                rewindIndex = -1;
                outerRows = null;
                outerRow = null;
                outerIndex = 0;
                emitOuterRows = false;
            }
            
            private void populateKeyValues(
                    ValuesExtractor valueExtractor,
                    EvaluationContext context,
                    Row row)
            {
                valueExtractor.extract(context, row, keyValues);
            }

            /** Batch outer rows and generate outer keys */
            private void batchOuterRows()
            {
                // Stop early to avoid allocation of lists
                if (!outerIt.hasNext())
                {
                    outerRows = emptyList();
                    return;
                }

                int count = 0;
                int size = batchSize > 0 ? batchSize : 100;
                outerRows = new ArrayList<>(size);
                while (outerIt.hasNext())
                {
                    Row row = outerIt.next();
                    populateKeyValues(outerValuesExtractor, context.getEvaluationContext(), row);
                    row.extractedValues = Arrays.copyOf(keyValues, keyValues.length);

                    // TODO: keep extracting after last until array diffs
                    //       to avoid fetching the same values twice from inner operator
                    
                    // Connect row to parent
//                    row.setPredicateParent(context.getParentRow());
                    outerRows.add(row);
                    count++;
                    if (batchSize > 0 && count >= batchSize)
                    {
                        break;
                    }
                }
            }
            
            private Iterator<Object[]> outerValuesIterator()
            {
                return new Iterator<Object[]>()
                {
                    private int outerRowsIndex = 0;
                    private Object[] nextArray;
                    private Object[] prevArray;

                    @Override
                    public boolean hasNext()
                    {
                        return setNext();
                    }

                    @Override
                    public Object[] next()
                    {
                        if (nextArray == null)
                        {
                            throw new NoSuchElementException();
                        }

                        Object[] result = nextArray;
                        nextArray = null;
                        return result;
                    }

                    private boolean setNext()
                    {
                        while (nextArray == null)
                        {
                            if (outerRowsIndex >= outerRows.size())
                            {
                                return false;
                            }

                            Row row = outerRows.get(outerRowsIndex++);
                            
                            // Skip already batched array
                            if (prevArray != null && Arrays.equals(prevArray, row.extractedValues))
                            {
                                continue;
                            }
                            
                            nextArray = row.extractedValues;
                            prevArray = nextArray;
                        }

                        return true;
                    }
                };
            }
        };
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * outer.hashCode() +
            37 * inner.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof BatchMergeJoin)
        {
            BatchMergeJoin mj = (BatchMergeJoin) obj;
            return outer.equals(mj.outer)
                && inner.equals(mj.inner)
                && outerValuesExtractor.equals(mj.outerValuesExtractor)
                && innerValuesExtractor.equals(mj.innerValuesExtractor)
                && predicate.equals(mj.predicate)
                && populating == mj.populating
                && emitEmptyOuterRows == mj.emitEmptyOuterRows;
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String description = String.format("BATCH MERGE JOIN (%s) (POPULATING: %s, OUTER: %s, EXECUTION COUNT: %s, BATCH SIZE: %s, PREDICATE: %s)",
                logicalOperator,
                populating,
                emitEmptyOuterRows,
                executionCount,
                batchSize,
                predicate);
        return description + System.lineSeparator()
            +
            indentString + outer.toString(indent + 1) + System.lineSeparator()
            +
            indentString + inner.toString(indent + 1);
    }
}
