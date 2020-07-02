package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;

import java.util.Iterator;

/** Definition of a selection operator */
public interface Operator
{
    /** Open iterator */
    Iterator<Row> open(ExecutionContext context);

    /**
     * To string with indent. Used when printing operator tree
     *
     * @param indent Indent count
     */
    default String toString(int indent)
    {
        return toString();
    }
}