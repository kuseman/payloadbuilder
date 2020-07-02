package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.OutputWriter;
import com.viskan.payloadbuilder.parser.ExecutionContext;

import org.apache.commons.lang3.ArrayUtils;

/** Definition of a projection */
public interface Projection
{
    static final Type[] EMPTY_TYPES = new Type[0];
    
    /** Return value for provided row */
    void writeValue(OutputWriter writer, ExecutionContext context);
    
    /** Get columns for this projection */
    default String[] getColumns()
    {
        return ArrayUtils.EMPTY_STRING_ARRAY;
    }
    
    /** Get columns for this projection */
    default Type[] getColumnTypes()
    {
        return EMPTY_TYPES;
    }
    
    public enum Type
    {
        OBJECT,
        ARRAY,
        VALUE
    }
}
