package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;

import static java.util.Collections.emptyIterator;
import static java.util.Objects.requireNonNull;
import static org.apache.commons.collections.CollectionUtils.isEmpty;

import java.util.Iterator;
import java.util.List;

/**
 * Batch operator. Used when utilizing a tables index to read values based on outer operators rows.
 */
public class BatchOperator implements Operator
{
    private final TableAlias alias;
    private final Reader reader;

    public BatchOperator(
            TableAlias alias,
            Reader reader)
    {
        this.alias = requireNonNull(alias, "alias");
        this.reader = requireNonNull(reader, "reader");
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        // No outer rows, scan?
        List<Row> outerRows = context.getOuterRows();
        if (isEmpty(outerRows))
        {
            return emptyIterator();
        }

        Iterator<Object[]> outerValuesIterator = outerValuesIterator(context.getEvaluationContext(), outerRows);
        return reader.open(context, alias, outerValuesIterator);
    }

//    private Iterator<Object[]> outerValuesIterator(EvaluationContext context, List<Row> outerRows)
//    {
//        return new Iterator<Object[]>()
//        {
//            private final int size = valuesExtractors.size();
//            // NOTE! Use a single array of values for all rows to minimize allocations
//            private final Object[] values = new Object[valuesExtractors.size()];
//            private int outerRowsIndex = 0;
//
//            @Override
//            public boolean hasNext()
//            {
//                return outerRowsIndex < outerRows.size();
//            }
//
//            @Override
//            public Object[] next()
//            {
//                Row row = outerRows.get(outerRowsIndex++);
//                for (int i = 0; i < size; i++)
//                {
//                    values[i] = valuesExtractors.get(i).apply(context, row);
//                }
//
//                return values;
//            }
//        };
//    }

    /** Definition of a batch reader, reading rows based on outer rows */
    public interface Reader
    {
        Iterator<Row> open(OperatorContext context, TableAlias alias, Iterator<Object[]> outerValues);
    }
}
