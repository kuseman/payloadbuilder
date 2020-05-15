package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiPredicate;
import java.util.function.ToIntBiFunction;

import org.apache.commons.lang3.StringUtils;

/**
 * <pre>
 * Special variant of merge join that merges two streams by first batching outer rows
 * And push these down to inner operator that in turn returns inner rows connected to the outer rows
 * in outer row order (see below).
 * 
 * Join used when there is a suitable index on both left and right side.
 * Index operator guarantees that rows is both unique regarding index columns
 * and also comes in order of the provided rows from outer stream.
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
 * </pre>
 **/
public class BatchMergeJoin implements Operator
{
    private final String logicalOperator;
    private final Operator outer;
    private final Operator inner;
    /** Hash function of outer values */
    private final ToIntBiFunction<EvaluationContext, Row> outerHashFunction;
    /** Hash function of inner values. */
    private final ToIntBiFunction<EvaluationContext, Row> innerHashFunction;
    private final BiPredicate<EvaluationContext, Row> predicate;
    private final RowMerger rowMerger;
    private final boolean populating;
    private final boolean emitEmptyOuterRows;
    private final int batchSize;

    /* Statistics */
    private int executionCount;

    // TODO: one-to-many  (unique is top) <- DONE!
    //       many-to-one  (unique is bottom) (BatchHashMatch)
    //       many-to-many (Not supported since there's only one type of index)
    /*
     * many-to-one
     * 
     * article_attribute (attr1_id)        attribute1 (attr1_id)
     * 1                                   
     * 10
     * 4
     * 2
     * 4
     * 10
     * 
     */
    
    public BatchMergeJoin(
            String logicalOperator,
            Operator outer,
            Operator inner,
            ToIntBiFunction<EvaluationContext, Row> outerHashFunction,
            ToIntBiFunction<EvaluationContext, Row> innerHashFunction,
            BiPredicate<EvaluationContext, Row> predicate,
            RowMerger rowMerger,
            boolean populating,
            boolean emitEmptyOuterRows,
            int batchSize)
    {
        this.logicalOperator = logicalOperator;
        this.outer = requireNonNull(outer, "outer");
        this.inner = requireNonNull(inner, "inner");
        this.outerHashFunction = requireNonNull(outerHashFunction, "outerHashFunction");
        this.innerHashFunction = requireNonNull(innerHashFunction, "innerHashFunction");
        this.predicate = requireNonNull(predicate, "predicate");
        this.rowMerger = requireNonNull(rowMerger, "rowMerger");
        this.populating = populating;
        this.emitEmptyOuterRows = emitEmptyOuterRows;
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
            private int outerIndex;
            private Iterator<Row> innerIt;

            private Row next;

            private Row prevOuterRow;
            private Row outerRow;
            private int currentOuterHash;
            private Row innerRow;
            private int currentInnerHash;
            private boolean outerRowEmited;

            @Override
            public Row next()
            {
                Row result = next;
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
                    if (outerRows == null)
                    {
                        batchOuterRows();
                        if (outerRows.isEmpty())
                        {
                            return false;
                        }

                        context.setOuterRows(outerRows);
                        innerIt = inner.open(context);
                    }

                    // Current outer rows are processed, clear and start over
                    if (outerIndex >= outerRows.size())
                    {
                        if (returnPopulatingOuterRow())
                        {
                            next = prevOuterRow;
                            prevOuterRow = null;
                            continue;
                        }

                        outerRow = null;
                        outerRows = null;
                        outerIndex = 0;
                        continue;
                    }

                    if (outerRow == null)
                    {
                        if (returnPopulatingOuterRow())
                        {
                            next = prevOuterRow;
                            prevOuterRow = null;
                            continue;
                        }

                        if (prevOuterRow != null)
                        {
                            prevOuterRow.clearPredicateParent();
                        }
                        
                        outerRow = outerRows.get(outerIndex);
                        currentOuterHash = outerHashFunction.applyAsInt(context.getEvaluationContext(), outerRow);
                        outerRowEmited = false;
                        prevOuterRow = outerRow;
                    }

                    if (innerRow == null)
                    {
                        // Set current outerRow as context parent before retrieving next inner row
                        Row contextParent = context.getParentRow();
                        context.setParentRow(outerRow);
                        innerRow = innerIt.hasNext() ? innerIt.next() : null;
                        context.setParentRow(contextParent);
                        if (innerRow != null)
                        {
                            currentInnerHash = innerHashFunction.applyAsInt(context.getEvaluationContext(), innerRow);
                        }
                    }

                    // No more inner rows, clear outerRow and start over
                    if (innerRow == null)
                    {
                        outerRow.clearPredicateParent();
                        outerRow = null;
                        outerIndex++;
                        continue;
                    }

                    innerRow.setPredicateParent(outerRow);

                    boolean hashMatch = currentOuterHash == currentInnerHash;

                    if (hashMatch
                        && predicate.test(context.getEvaluationContext(), innerRow))
                    {
                        next = rowMerger.merge(outerRow, innerRow, populating);
                        outerRowEmited = true;
                        if (populating)
                        {
                            outerRow.match = true;
                            next = null;
                        }
                    }

                    innerRow.clearPredicateParent();

                    // Move outer row if no values match.
                    // Since it's by contract that inner operator
                    // return rows in order of the outer rows, we can safely
                    // iterate the outer rows until a match if found
                    if (!hashMatch)
                    {
                        if (returnNonPopulatingOuterRow())
                        {
                            next = outerRow;
                        }
                        outerRow.clearPredicateParent();
                        outerRow = null;
                        outerIndex++;
                    }
                    // Otherwise move inner row
                    else
                    {
                        innerRow = null;
                    }
                }
                return true;
            }

            private boolean returnNonPopulatingOuterRow()
            {
                return emitEmptyOuterRows
                    && !populating
                    && !outerRowEmited;
            }

            private boolean returnPopulatingOuterRow()
            {
                return populating
                    && prevOuterRow != null
                    && (prevOuterRow.match
                        || emitEmptyOuterRows);
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
                    // Connect row to parent
                    row.setPredicateParent(context.getParentRow());
                    outerRows.add(row);
                    count++;
                    if (batchSize > 0 && count >= batchSize)
                    {
                        break;
                    }
                }
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
                && outerHashFunction.equals(mj.outerHashFunction)
                && innerHashFunction.equals(mj.innerHashFunction)
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
        String description = String.format("OUTER KEYS MERGE JOIN (%s) (POPULATING: %s, OUTER: %s, EXECUTION COUNT: %s, BATCH SIZE: %s, PREDICATE: %s)",
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
