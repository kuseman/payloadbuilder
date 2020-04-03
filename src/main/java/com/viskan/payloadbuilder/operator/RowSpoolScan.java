package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;

/** Scan rows from spool with provided key */
public class RowSpoolScan implements Operator
{
    public static final RowSpoolScan PARENTS_SPOOL_SCAN = new RowSpoolScan("parents");
    
    private final String key;

    public RowSpoolScan(String key)
    {
        this.key = requireNonNull(key, "key");
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        return context.getSpoolRows(key).iterator();
    }
    
    @Override
    public int hashCode()
    {
        return key.hashCode();
    }
    
    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof RowSpoolScan)
        {
            return key.equals(((RowSpoolScan) obj).key);
        }
        return false;
    }
    
    @Override
    public String toString()
    {
        return "SPOOL SCAN (" + key + ")";
    }
}
