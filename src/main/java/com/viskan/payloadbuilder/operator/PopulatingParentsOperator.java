package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;

import org.apache.commons.lang3.StringUtils;

/** Streams parent rows from downstream operator rows */
public class PopulatingParentsOperator implements Operator
{
    private final Operator operator;
    /** Should parents childrows be populated */
    private final boolean populateChildRows;
    /** Key used to fetch non matching parent rows if {@link #outer} is set to true */
//    private final String spoolKey;
//    /** Outer populating join, stream non matched parent rows from spool */
//    private final boolean outer;

    public PopulatingParentsOperator(Operator operator, boolean populateChildRows/*, boolean outer, String spoolKey*/)
    {
        this.operator = requireNonNull(operator, "operator");
        this.populateChildRows = populateChildRows;
//        this.spoolKey = requireNonNull(spoolKey, "spoolKey");
//        this.outer = outer;
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        final Iterator<Row> it = operator.open(context);

        return new Iterator<Row>()
        {
            private Row currentChild;
            private Iterator<Row> parentIt;
            private Row next;
            private Row prev;

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

            boolean setNext()
            {
                while (next == null)
                {
                    if (currentChild == null)
                    {
                        if (!it.hasNext())
                        {
                            // Return left over
                            if (prev != null)
                            {
                                next = prev;
                                prev = null;
                                continue;
                            }
                            
                            return false;
                        }

                        currentChild = it.next();
                        parentIt = currentChild.getParents().iterator();
                        continue;
                    }
                    else if (!parentIt.hasNext())
                    {
                        parentIt = null;
                        currentChild = null;
                        continue;
                    }
                    
                    Row parent = parentIt.next();
                    if (populateChildRows)
                    {
                        parent.getChildRows(currentChild.getTableAlias().getParentIndex()).add(currentChild);
                    }
                    
                    // Position changed, return prev
                    if (prev != null && prev.getPos() != parent.getPos())
                    {
                        next = prev;
                    }
                    prev = parent;
                }

                return next != null;
            }
        };
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        String desc = String.format("POPULATING PARENT ROWS (POPUALTE_CHILD_ROWS=%s)", populateChildRows);
        return desc + System.lineSeparator() +
            indentString + operator.toString(indent + 1);
    }
}
