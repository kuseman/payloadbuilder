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
    private final BiPredicate<ExecutionContext, Tuple> predicate;
    private final TupleMerger rowMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;
    private final Index innerIndex;
    private final int valuesSize;
    private final int batchSize;

    // TODO: min and max batch size (merge and hash)
    //       to guard against to large batches even if all values are the same

    //CSOFF
    BatchMergeJoin(
    //CSON
            int nodeId,
            String logicalOperator,
            Operator outer,
            Operator inner,
            ValuesExtractor outerValuesExtractor,
            ValuesExtractor innerValuesExtractor,
            BiPredicate<ExecutionContext, Tuple> predicate,
            TupleMerger rowMerger,
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

    //CSOFF
    @Override
    //CSON
    public RowIterator open(ExecutionContext context)
    {
        final RowIterator outerIt = outer.open(context);
        final JoinTuple joinTuple = new JoinTuple();
        joinTuple.setContextOuter(context.getTuple());
        //CSOFF
        return new RowIterator()
        //CSON
        {
            /** Batched rows */
            private List<TupleHolder> outerRows;
            private int outerIndex;
            private TupleHolder batchedLeftOverRow;
            private Iterator<Tuple> innerIt;

            /** Reference to outer values iterator to verify that implementations of Operator fully uses the index if specified */
            private Iterator<Object[]> outerValuesIterator;

            private Tuple next;
            /** Current process outer row */
            private TupleHolder outerRow;
            /** Current process inner row */
            private Tuple innerRow;

            private final Object[] keyValues = new Object[valuesSize];

            /**
             * <pre>
             * Index pointing to last seen larger than value for when outerRow > innerRow.
             * Used in many-mode when rewinding outer rows.
             * </pre>
             */
            private int largerThanIndex;
            /** Index of outerRows where to rewind in case of man-mode */
            private int rewindIndex = -1;

            /** Flag used when having populating join or, left join */
            private boolean emitOuterRows;

            @Override
            public Tuple next()
            {
                Tuple result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            //CSOFF
            private boolean setNext()
            //CSON
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
                            //CSOFF
                            if (emitEmptyOuterRows)
                            //CSON
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
                            //CSOFF
                            if (populating)
                            //CSON
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
                            //CSOFF
                            if (!populating && emitEmptyOuterRows)
                            //CSON
                            {
                                next = outerRow.tuple;
                            }
                            outerIndex++;
                        }
                        outerRow = null;
                        continue;
                    }

                    joinTuple.setOuter(outerRow.tuple);
                    joinTuple.setInner(innerRow);

                    if (predicate.test(context, joinTuple))
                    {
                        next = rowMerger.merge(outerRow.tuple, innerRow, populating, nodeId);
                        outerRow.match = true;
                        if (populating)
                        {
                            outerRow.tuple = next;
                            next = null;
                        }
                    }

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

            private int compare(ExecutionContext context, TupleHolder outerRow, Tuple innerRow)
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

                TupleHolder tuple = outerRows.get(outerIndex);
                // Populating and row is a match of empty outer rows should be emitted
                // Or non populating and no match
                if ((populating && (tuple.match || emitEmptyOuterRows))
                    || (!populating && !tuple.match))
                {
                    next = tuple.tuple;
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
                    Tuple tuple)
            {
                valueExtractor.extract(context, tuple, keyValues);
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
                    Tuple tuple = outerIt.next();
                    populateKeyValues(outerValuesExtractor, context, tuple);

                    TupleHolder holder = new TupleHolder(tuple);
                    holder.extractedValues = Arrays.copyOf(keyValues, keyValues.length);

                    if (batchSize > 0 && count >= batchSize)
                    {
                        // Keep batching until values values start to diff
                        // this to avoid fetching the same data twice from downstream
                        if (count > batchSize && !Arrays.equals(prevKeyValues, keyValues))
                        {
                            // Save row for next iteration
                            batchedLeftOverRow = holder;
                            break;
                        }
                    }
                    outerRows.add(holder);
                    count++;

                    prevKeyValues = holder.extractedValues;
                }
            }

            private Iterator<Object[]> outerValuesIterator()
            {
                //CSOFF
                return new Iterator<Object[]>()
                //CSON
                {
                    private int outerRowsIndex;
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

                            TupleHolder tuple = outerRows.get(outerRowsIndex++);

                            // Skip already batched array
                            if (prevArray != null && Arrays.equals(prevArray, tuple.extractedValues))
                            {
                                continue;
                            }

                            nextArray = tuple.extractedValues;
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
        //CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + outer.hashCode();
        hashCode = hashCode * 37 + inner.hashCode();
        return hashCode;
        //CSON
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

    /** Temporary holder for tuples during join */
    static class TupleHolder
    {
        private Tuple tuple;
        private Object[] extractedValues;
        private boolean match;

        TupleHolder(Tuple tuple)
        {
            this.tuple = tuple;
        }
    }
}
