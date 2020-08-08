package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;

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
class BatchMergeJoin extends AOperator
{
    private final String logicalOperator;
    private final Operator outer;
    private final Operator inner;
    private final ValuesExtractor outerValuesExtractor;
    private final ValuesExtractor innerValuesExtractor;
    private final BiPredicate<ExecutionContext, Row> predicate;
    private final RowMerger rowMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;
    private final Index innerIndex;
    private final int valuesSize;
    private final int batchSize;

    // TODO: min and max batch size (merge and hash)
    //       to guard against to large batches even if all values are the same

    BatchMergeJoin(
            int nodeId,
            String logicalOperator,
            Operator outer,
            Operator inner,
            ValuesExtractor outerValuesExtractor,
            ValuesExtractor innerValuesExtractor,
            BiPredicate<ExecutionContext, Row> predicate,
            RowMerger rowMerger,
            boolean populating,
            boolean emitEmptyOuterRows,
            Index innerIndex,
            int batchSize)
    {
        super(nodeId);
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

    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        final Iterator<Row> outerIt = outer.open(context);
        return new Iterator<Row>()
        {
            /** Batched rows */
            private List<Row> outerRows;
            private int outerIndex = 0;
            private Row batchedLeftOverRow;
            private Iterator<Row> innerIt;

            /** Reference to outer values iterator to verify that implementations of Operator fully uses the index if specified */
            private Iterator<Object[]> outerValuesIterator;

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
                        emitOuterRows();
                        continue;
                    }

                    if (outerRows == null || outerRows.isEmpty())
                    {
                        batchOuterRows();
                        if (outerRows.isEmpty())
                        {
                            verifyOuterValuesIterator();
                            context.getOperatorContext().setOuterIndexValues(null);
                            return false;
                        }

                        outerValuesIterator = outerValuesIterator();
                        context.getOperatorContext().setOuterIndexValues(outerValuesIterator);
                        innerIt = inner.open(context);
                        if (!innerIt.hasNext())
                        {
                            verifyOuterValuesIterator();
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
                            verifyOuterValuesIterator();

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

                    int cResult = compare(context, outerRow, innerRow);

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
                    if (predicate.test(context, innerRow))
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

            private int compare(ExecutionContext context, Row outerRow, Row innerRow)
            {
                Object[] outerValues = outerRow.extractedValues;
                populateKeyValues(innerValuesExtractor, context, innerRow);
                Object[] innerValues = keyValues;

                int length = outerValues.length;

                for (int i = 0; i < length; i++)
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

            private void verifyOuterValuesIterator()
            {
                if (outerValuesIterator != null && outerValuesIterator.hasNext())
                {
                    throw new IllegalArgumentException("Check implementation of operator with index: " + innerIndex + " not all outer values was processed.");
                }
            }

            private void emitOuterRows()
            {
                // Complete
                if (outerIndex > outerRows.size() - 1)
                {
                    clearOuterRows();
                    return;
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
                    ExecutionContext context,
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
                Object[] prevKeyValues = null;
                if (batchedLeftOverRow != null)
                {
                    outerRows.add(batchedLeftOverRow);
                    prevKeyValues = batchedLeftOverRow.extractedValues;
                    batchedLeftOverRow = null;
                    count++;
                }

                while (outerIt.hasNext())
                {
                    Row row = outerIt.next();
                    populateKeyValues(outerValuesExtractor, context, row);
                    row.extractedValues = Arrays.copyOf(keyValues, keyValues.length);

                    if (batchSize > 0 && count >= batchSize)
                    {
                        // Keep batching until values values start to diff
                        // this to avoid fetching the same data twice from downstream
                        if (count > batchSize && !Arrays.equals(prevKeyValues, keyValues))
                        {
                            // Save row for next iteration
                            batchedLeftOverRow = row;
                            break;
                        }
                    }
                    outerRows.add(row);
                    count++;

                    prevKeyValues = row.extractedValues;
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

    static void clearJoinData(Row row)
    {
        row.hash = 0;
        row.extractedValues = null;
        row.match = false;
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
            BatchMergeJoin that = (BatchMergeJoin) obj;
            return nodeId == that.nodeId
                && outer.equals(that.outer)
                && inner.equals(that.inner)
                && outerValuesExtractor.equals(that.outerValuesExtractor)
                && innerValuesExtractor.equals(that.innerValuesExtractor)
                && predicate.equals(that.predicate)
                && populating == that.populating
                && emitEmptyOuterRows == that.emitEmptyOuterRows;
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String description = String.format("BATCH MERGE JOIN (%s) (ID: %d, POPULATING: %s, OUTER: %s, BATCH SIZE: %s, PREDICATE: %s)",
                logicalOperator,
                nodeId,
                populating,
                emitEmptyOuterRows,
                batchSize,
                predicate);
        return description + System.lineSeparator()
            +
            indentString + outer.toString(indent + 1) + System.lineSeparator()
            +
            indentString + inner.toString(indent + 1);
    }
}