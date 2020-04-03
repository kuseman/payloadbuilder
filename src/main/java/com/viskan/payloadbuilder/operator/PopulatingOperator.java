package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.parser.tree.PopulateTableSource;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.StringUtils;

/**
 * Operator used on {@link PopulateTableSource}'s. Performs join and streams child rows for provided child index. If this is an outer join, non matching
 * parents are spool and retrieved at a later stage.
 **/
public class PopulatingOperator implements Operator
{
    /** Top join of the populating join {@link PopulateTableSource#getTableSource()} */
    private final Operator join;
    private final int childIndex;
    private final boolean outer;
    private final String spoolKey;

    public PopulatingOperator(Operator join, int childIndex, boolean outer, String spoolKey)
    {
        this.join = join;
        this.childIndex = childIndex;
        this.outer = outer;
        this.spoolKey = spoolKey;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        final Iterator<Row> it = join.open(context);

        return new Iterator<Row>()
        {
            private Row next;
            private Iterator<Row> childIt;
            private List<Row> nonMatchingRows;
            
            @Override
            public boolean hasNext()
            {
                return next != null || setNext();
            }
            
            @Override
            public Row next()
            {
                Row result = next;
                next = null;
                return result;
            }
            
            private boolean setNext()
            {
                while (next == null)
                {
                    if (childIt == null)
                    {
                        if (!it.hasNext())
                        {
                            return false;
                        }
                        
                        Row row = it.next();
                        List<Row> childRows = row.getChildRows(childIndex);
                        if (childRows.isEmpty() && outer)
                        {
                            if (nonMatchingRows == null)
                            {
                                nonMatchingRows = new ArrayList<>();
                                context.storeSpoolRows(spoolKey, nonMatchingRows);
                            }
                            nonMatchingRows.add(row);
                        }
                        childIt = childRows.iterator();
                        continue;
                    }
                    else if (!childIt.hasNext())
                    {
                        childIt = null;
                        continue;
                    }
                    
                    next = childIt.next();
                }
                
                return next != null;
            }
        };
//        
//        return new ObjectGraphIterator(it, new Transformer()
//        {
//            private Iterator<Row> childIt;
//            private List<Row> nonMatchingRows = null;
//
//            @Override
//            public Object transform(Object input)
//            {
//                Row row = (Row) input;
//                if (childIt == null)
//                {
//                    List<Row> childRows = row.getChildRows(childIndex);
//                    if (childRows.isEmpty() && outer)
//                    {
//                        if (nonMatchingRows == null)
//                        {
//                            nonMatchingRows = new ArrayList<>();
//                            context.storeSpoolRows(spoolKey, nonMatchingRows);
//                        }
//                        nonMatchingRows.add(row);
//                    }
//                    childIt = childRows.iterator();
//                    return childIt;
//                }
//                else if (!childIt.hasNext())
//                {
//                    childIt = null;
//                }
//                return row;
//            }
//        });
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String desc = String.format("POPULATING (CHILD INDEX=%s, OUTER=%s, SPOOL KEY=%s)", childIndex, outer, spoolKey);
        return desc + System.lineSeparator() +
            indentString + join.toString(indent + 1);
    }
}
