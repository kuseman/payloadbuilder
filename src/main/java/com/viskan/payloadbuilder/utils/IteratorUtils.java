package com.viskan.payloadbuilder.utils;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.List;

public class IteratorUtils
{
    private IteratorUtils()
    {
    }

    /**
     * Returns an iterator that iterates all parents child rows for a specific child rows index.
     */
    public Iterable<Row> getChildRowsIterator(List<Row> parents, int childRowsIndex)
    {
        requireNonNull(parents, "parents");
        return new ChildRowsIteraor(parents, childRowsIndex);
    }

    /** */
    private static class ChildRowsIteraor implements Iterable<Row>
    {
        private final List<Row> parentRows;
        private final int childIndex;

        ChildRowsIteraor(List<Row> parentRows, int childIndex)
        {
            this.parentRows = parentRows;
            this.childIndex = childIndex;
        }

        @Override
        public Iterator<Row> iterator()
        {
            return new Iterator<Row>()
            {
                Row next;
                int parentIndex = 0;
                List<Row> childRows;
                int currentChildIndex = 0;

                @Override
                public Row next()
                {
                    Row r = next;
                    next = null;
                    return r;
                }

                @Override
                public boolean hasNext()
                {
                    return next != null || setNext();
                }

                boolean setNext()
                {
                    while (next == null)
                    {
                        if (childRows == null)
                        {
                            if (parentIndex >= parentRows.size())
                            {
                                return false;
                            }

                            Row parentRow = parentRows.get(parentIndex++);
                            childRows = parentRow.getChildRows(childIndex);
                            currentChildIndex = 0;
                        }
                        else if (currentChildIndex >= childRows.size())
                        {
                            childRows = null;
                            continue;
                        }
                        next = childRows.get(currentChildIndex++);
                    }

                    return next != null;
                }
            };
        }
    }
}
