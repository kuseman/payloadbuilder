package org.kuse.payloadbuilder.core.operator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Stack;

import org.apache.commons.collections.IteratorUtils;
import org.kuse.payloadbuilder.core.OutputWriter;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Output writer that builds an internal object structure with values/lists/maps */
public abstract class AObjectOutputWriter implements OutputWriter
{
    private final Stack<Object> complextValueStack = new Stack<>();
    private List<ColumnValue> row;
    private String column;
    private boolean rootObject = true;

    /** Consume current result row */
    protected abstract void consumeRow(List<ColumnValue> row);

    @Override
    public void startRow()
    {
        rootObject = true;
        row = new ArrayList<>();
        complextValueStack.clear();
    }

    @Override
    public void endRow()
    {
        consumeRow(row);
    }

    @Override
    public void writeFieldName(String name)
    {
        column = name;
    }

    @Override
    public void writeValue(Object value)
    {
        Object result = unwrap(value);

        // Root
        if (complextValueStack.size() == 0)
        {
            row.add(new ColumnValue(column, result));
            return;
        }

        appendToComplextValue(complextValueStack.peek(), column, result);
    }

    @Override
    public void startObject()
    {
        // Skip the root object
        if (rootObject)
        {
            rootObject = false;
            return;
        }

        Object complexValue = complextValueStack.size() > 0 ? complextValueStack.peek() : null;
        Object object = complextValueStack.push(new LinkedHashMap<>());
        // This was the added on root add it to result
        if (complexValue == null)
        {
            row.add(new ColumnValue(column, object));
        }
        else
        {
            appendToComplextValue(complexValue, column, object);
        }
    }

    @Override
    public void endObject()
    {
        if (complextValueStack.size() > 0)
        {
            complextValueStack.pop();
        }
    }

    @Override
    public void startArray()
    {
        Object complexValue = complextValueStack.size() > 0 ? complextValueStack.peek() : null;
        Object object = complextValueStack.push(new ArrayList<>());
        // This was the added on root add it to result
        if (complexValue == null)
        {
            row.add(new ColumnValue(column, object));
        }
        else
        {
            appendToComplextValue(complexValue, column, object);
        }
    }

    @Override
    public void endArray()
    {
        complextValueStack.pop();
    }

    @SuppressWarnings("unchecked")
    private void appendToComplextValue(Object complexValue, String column, Object value)
    {
        if (complexValue instanceof Map)
        {
            ((Map<String, Object>) complexValue).put(column, value);
        }
        else
        {
            ((List<Object>) complexValue).add(value);
        }
    }

    @SuppressWarnings("unchecked")
    private Object unwrap(Object value)
    {
        if (value instanceof Iterator)
        {
            return IteratorUtils.toList((Iterator<Object>) value);
        }
        return value;
    }

    /** Result of a cell in a result set on root level */
    public static class ColumnValue
    {
        private final String key;
        private final Object value;

        @JsonCreator
        public ColumnValue(@JsonProperty("key") String key, @JsonProperty("value") Object value)
        {
            this.key = key;
            this.value = value;
        }

        public String getKey()
        {
            return key;
        }

        public Object getValue()
        {
            return value;
        }

        @Override
        public int hashCode()
        {
            return Objects.hash(key, value);
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof ColumnValue)
            {
                ColumnValue that = (ColumnValue) obj;
                return Objects.equals(key, that.key)
                    && Objects.equals(value, that.value);
            }
            return false;
        }

        @Override
        public String toString()
        {
            return key + "=" + value;
        }
    }
}
