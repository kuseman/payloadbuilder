package se.kuseman.payloadbuilder.core.operator;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Deque;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import org.apache.commons.collections4.IteratorUtils;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import se.kuseman.payloadbuilder.api.OutputWriter;

/** Output writer that builds an internal object structure with values/lists/maps */
public abstract class AObjectOutputWriter implements OutputWriter
{
    private final Deque<Object> complexValueStack = new ArrayDeque<>();
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
        complexValueStack.clear();
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
        if (complexValueStack.size() == 0)
        {
            row.add(new ColumnValue(column, result));
            return;
        }

        appendToComplextValue(complexValueStack.peekFirst(), column, result);
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

        Object complexValue = complexValueStack.size() > 0 ? complexValueStack.peekFirst()
                : null;
        Object object = new LinkedHashMap<>();
        complexValueStack.addFirst(object);
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
        if (complexValueStack.size() > 0)
        {
            complexValueStack.removeFirst();
        }
    }

    @Override
    public void startArray()
    {
        Object complexValue = complexValueStack.size() > 0 ? complexValueStack.peekFirst()
                : null;
        Object object = new ArrayList<>();
        complexValueStack.addFirst(object);
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
        complexValueStack.pop();
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
