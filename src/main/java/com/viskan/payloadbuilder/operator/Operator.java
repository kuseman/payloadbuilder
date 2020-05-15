package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableObject;

import static java.util.Collections.emptyList;

import java.util.Iterator;
import java.util.List;

/** Definition of a selection operator */
public interface Operator
{
    /** Open iterator */
    Iterator<Row> open(OperatorContext context);

    /**
     * To string with indent. Used when printing operator tree
     *
     * @param indent Indent count
     */
    default String toString(int indent)
    {
        return toString();
    }
    
    /** Returns estimated row size */
    default int getEstimatedRowSize()
    {
        return -1;
    }
    
    /** Return sort items that this operator come sorted by */
    default List<TableObject> getSortItems()
    {
        return emptyList();
    }
}