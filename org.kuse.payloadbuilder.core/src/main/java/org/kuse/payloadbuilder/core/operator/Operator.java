package org.kuse.payloadbuilder.core.operator;

import static java.util.Collections.emptyList;
import static java.util.Collections.emptyMap;

import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.parser.ExecutionContext;

/** Definition of a selection operator */
public interface Operator
{
    /** Open iterator */
    Iterator<Row> open(ExecutionContext context);

    /** Get node id of operator */
    int getNodeId();
    
    /** Return child operators if any */
    default List<Operator> getChildOperators()
    {
        return emptyList();
    }
    
    /** Returns name of operator when used in describe/analyze statements etc. */
    default String getName()
    {
        return getClass().getSimpleName();
    }
    
    /** Returns a short describe string of the operator.
     * Used in describe statement */
    default String getDescribeString()
    {
        return "";
    }
    
    /** Returns more detail properties of describe statement if {@link #getDescribeString()} is not enough. */
    default Map<String, Object> getDescribeProperties()
    {
        return emptyMap();
    }
    
    /** Returns a short analyze string of the operator.
     * Used in analyze statement */
    default String getAnalyzeString()
    {
        return "";
    }
    
    /** Returns more detail properties of analyze statement if {@link #getAnalyzeString()} is not enough. */
    default Map<String, Object> getAnalyzeProperties()
    {
        return emptyMap();
    }
    
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