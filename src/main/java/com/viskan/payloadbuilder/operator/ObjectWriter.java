package com.viskan.payloadbuilder.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Stack;

import org.apache.commons.lang3.tuple.Pair;

/** 
 * <pre>
 * Writer that writes output as object. 
 * - Object - Map
 * - Array - List 
 * </pre>
 **/
public class ObjectWriter implements OutputWriter
{
    private final Stack<Object> parent = new Stack<>();
    private final Stack<String> currentField = new Stack<>();
    
    /** Returns written value and clears state */
    public Object getValue()
    {
        currentField.clear();
        return parent.pop();
    }
    
    @Override
    public void writeFieldName(String name)
    {
        currentField.push(name);
    }

    @Override
    public void writeValue(Object input)
    {
        Object value = input;
        if (value instanceof Iterator)
        {
            @SuppressWarnings("unchecked")
            Iterator<Object> it = (Iterator<Object>) value;
            startArray();
            while (it.hasNext())
            {
                writeValue(it.next());
            }
            endArray();
            return;
        }
        
        putValue(value);
    }

    @Override
    public void startObject()
    {
        parent.push(new PairList());
    }

    @Override
    public void endObject()
    {
        putValue(parent.pop());
    }

    @Override
    public void startArray()
    {
        parent.push(new ArrayList<>());
    }

    @Override
    public void endArray()
    {
        putValue(parent.pop());
    }
    
    @SuppressWarnings("unchecked")
    private void putValue(Object value)
    {
        // Top of stack put value back
        if (parent.isEmpty())
        {
            parent.push(value);
            return;
        }
        
        Object p = parent.peek();
        
        if (p instanceof PairList)
        {
            ((PairList) p).add(Pair.of(currentField.pop(), value));
        }
        else if (p instanceof List)
        {
            ((List<Object>) p).add(value);
        }
    }

    private static class PairList extends ArrayList<Pair<String, Object>>
    {}
}
