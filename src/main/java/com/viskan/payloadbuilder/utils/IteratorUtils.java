package com.viskan.payloadbuilder.utils;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Collection;
import java.util.Enumeration;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.collections.iterators.ArrayIterator;
import org.apache.commons.collections.iterators.EnumerationIterator;
import org.apache.commons.collections.iterators.ObjectArrayIterator;
import org.apache.commons.collections.iterators.SingletonIterator;

public class IteratorUtils
{
    private IteratorUtils()
    {
    }

    /** Get iterator from provided object */
    @SuppressWarnings("unchecked")
    public static Iterator<Object> getIterator(Object obj)
    {
        if (obj instanceof Iterator)
        {
            return (Iterator) obj;
        }
        else if (obj instanceof Collection)
        {
            return ((Collection) obj).iterator();

        }
        else if (obj instanceof Object[])
        {
            return new ObjectArrayIterator((Object[]) obj);

        }
        else if (obj instanceof Enumeration)
        {
            return new EnumerationIterator((Enumeration) obj);
        }
        else if (obj != null && obj.getClass().isArray())
        {
            return new ArrayIterator(obj);
        }

        return new SingletonIterator(obj);
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
