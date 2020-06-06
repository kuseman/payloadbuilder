package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.Index;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static com.viskan.payloadbuilder.operator.BatchMergeJoin.clearJoinData;
import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.BiPredicate;

import org.apache.commons.lang3.StringUtils;

import gnu.trove.map.TIntObjectMap;
import gnu.trove.map.hash.TIntObjectHashMap;

/**
 * <pre>
 * Special variant of hash join that merges two streams by first batching outer rows
 * And push these down to inner operator that in turn returns inner rows connected to the outer rows
 *
 * Then hashing is performed like a regular hash join
 *
 * from source s
 * inner join article a
 *   on a.art_id = sqrt(s.value)
 *
 * hash (sqrt(s.value))
 * probe a.art_id
 * </pre>
 **/
public class BatchHashJoin implements Operator
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
    private final int batchSize;
    private final Index innerIndex;
    private final int valuesSize;

    /* Statistics */
    private int executionCount;

    public BatchHashJoin(
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
        this.logicalOperator = requireNonNull(logicalOperator, "logicalOperator");
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
    public Iterator<Row> open(OperatorContext context)
    {
        executionCount++;
        final Iterator<Row> outerIt = outer.open(context);
        return new Iterator<Row>()
        {
            /** Batched rows */
            private List<Row> outerRows;
            private int outerRowIndex;
            private List<Row> innerRows;
            private int innerRowIndex;

            /** Reference to outer values iterator to verify that implementations of Operator fully uses the index if specified */
            private Iterator<Object[]> outerValuesIterator;

            /** Table use for hashed inner values */
            private final TIntObjectMap<TableValue> table = new TIntObjectHashMap<>((int) (batchSize * 1.5));

            private Row next;
            private Row outerRow;

            private final Object[] keyValues = new Object[valuesSize];

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

                    if (outerRows == null)
                    {
                        batchOuterRows();
                        if (outerRows.isEmpty())
                        {
                            verifyOuterValuesIterator();
                            return false;
                        }

                        hashInnerBatch();
                        // Start probing
                        continue;
                    }
                    // Probe current outer row
                    else if (innerRows == null)
                    {
                        // We're done
                        if (outerRowIndex >= outerRows.size())
                        {
                            // Populating mode, emit outer rows
                            if (populating)
                            {
                                outerRowIndex = 0;
                                emitOuterRows = true;
                            }
                            else
                            {
                                clearOuterRows();
                            }
                            continue;
                        }

                        outerRow = outerRows.get(outerRowIndex);

                        TableValue tableValue = table.get(outerRow.hash);
                        innerRows = tableValue.getRows();
                        if (innerRows.isEmpty())
                        {
                            if (!populating && emitEmptyOuterRows)
                            {
                                next = outerRow;
                            }

                            innerRows = null;
                            outerRowIndex++;
                            continue;
                        }
                    }
                    else if (innerRowIndex >= innerRows.size())
                    {
                        outerRowIndex++;
                        innerRows = null;
                        innerRowIndex = 0;
                        continue;
                    }

                    Row innerRow = innerRows.get(innerRowIndex++);
                    innerRow.setPredicateParent(outerRow);

                    if (predicate.test(context.getEvaluationContext(), innerRow))
                    {
                        next = rowMerger.merge(outerRow, innerRow, populating);
                        if (populating)
                        {
                            outerRow.match = true;
                            next = null;
                        }
                    }

                    innerRow.clearPredicateParent();
                }
                return true;
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
                    outerRows.add(row);
                    count++;
                    if (batchSize > 0 && count >= batchSize)
                    {
                        break;
                    }
                }
            }

            private void hashInnerBatch()
            {
                outerValuesIterator = outerValuesIterator(context.getEvaluationContext());
                context.setIndex(innerIndex, outerValuesIterator);
                Iterator<Row> it = inner.open(context);

                // Hash batch
                while (it.hasNext())
                {
                    Row row = it.next();
                    int hash = populateKeyValues(innerValuesExtractor, context.getEvaluationContext(), row);
                    TableValue tableValue = table.get(hash);
                    if (tableValue == null)
                    {
                        // No outer row exists for this inner rows hash, no need to add it
                        continue;
                    }
                    tableValue.addRow(row);
                }
                
                verifyOuterValuesIterator();
                context.setIndex(null, null);
            }
            
            private void emitOuterRows()
            {
                // Complete
                if (outerRowIndex > outerRows.size() - 1)
                {
                    clearOuterRows();
                    return;
                }

                Row row = outerRows.get(outerRowIndex);
                if (row.match || emitEmptyOuterRows)
                {
                    next = row;
                }

                outerRowIndex++;
            }
            
            private void verifyOuterValuesIterator()
            {
                if (outerValuesIterator != null && outerValuesIterator.hasNext())
                {
                    throw new IllegalArgumentException("Check implementation of operator with index: " + innerIndex + " not all outer values was processed.");
                }
            }

            private void clearOuterRows()
            {
                outerRow = null;
                outerRows = null;
                outerRowIndex = 0;
                emitOuterRows = false;
            }

            private int populateKeyValues(
                    ValuesExtractor valueExtractor,
                    EvaluationContext context,
                    Row row)
            {
                valueExtractor.extract(context, row, keyValues);
                return hash(keyValues);
            }

            private Iterator<Object[]> outerValuesIterator(EvaluationContext context)
            {
                return new Iterator<Object[]>()
                {
                    private int outerRowsIndex = 0;
                    private Object[] nextArray;

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
                            row.hash = populateKeyValues(outerValuesExtractor, context, row);
                            TableValue tableValue = table.get(row.hash);
                            if (tableValue == null)
                            {
                                tableValue = new TableValue();
                                table.put(row.hash, tableValue);
                            }
                            
                            if (!tableValue.addInnterValues(keyValues))
                            {
                                // Value already present, no need to push values to downstream
                                continue;
                            }

                            nextArray = keyValues;
                        }

                        return true;
                    }
                };
            }
        };
    }
    
    private int hash(Object[] values)
    {
        int result = 1;

        int length = values.length;
        for (int i = 0; i < length; i++)
        {
            Object value = values[i];
            result = 31 * result + (value == null ? 0 : value.hashCode());
        }
        return result;
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
        if (obj instanceof BatchHashJoin)
        {
            BatchHashJoin that = (BatchHashJoin) obj;
            return logicalOperator.equals(that.logicalOperator)
                && outer.equals(that.outer)
                && inner.equals(that.inner)
                && outerValuesExtractor.equals(that.outerValuesExtractor)
                && innerValuesExtractor.equals(that.innerValuesExtractor)
                && predicate.equals(that.predicate)
                && rowMerger.equals(that.rowMerger)
                && populating == that.populating
                && emitEmptyOuterRows == that.emitEmptyOuterRows
                && batchSize == that.batchSize;
        }
        return false;
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String description = String.format("BATCH HASH JOIN (%s) (POPULATING: %s, OUTER: %s, EXECUTION COUNT: %s, BATCH SIZE: %s, INDEX: %s, OUTER VALUES: %s, INNER VALUES: %s, PREDICATE: %s)",
                logicalOperator,
                populating,
                emitEmptyOuterRows,
                executionCount,
                batchSize,
                innerIndex,
                outerValuesExtractor,
                innerValuesExtractor,
                predicate);
        return description + System.lineSeparator()
            +
            indentString + outer.toString(indent + 1) + System.lineSeparator()
            +
            indentString + inner.toString(indent + 1);
    }
    
    /** Value used in table */
    private static class TableValue
    {
        private List<Row> rows;
        private List<Object[]> values;

        /** Check if provided key exists in values */
        private boolean contains(Object[] keyValues)
        {
            int size = values.size();
            for (int i = 0; i < size; i++)
            {
                if (Arrays.equals(values.get(i), keyValues))
                {
                    return true;
                }
            }
            return false;
        }

        public void addRow(Row row)
        {
            if (rows == null)
            {
                rows = new ArrayList<>();
            }
            
            rows.add(row);
        }

        public List<Row> getRows()
        {
            return rows != null ? rows : emptyList();
        }

        /** Add inner values array
         * @return True if array was added */
        boolean addInnterValues(Object[] keyValues)
        {
            if (values == null)
            {
                values = new ArrayList<>();
            }
            else if (contains(keyValues))
            {
                return false;
            }
            
            values.add(Arrays.copyOf(keyValues, keyValues.length));
            return true;
        }
    }
}
