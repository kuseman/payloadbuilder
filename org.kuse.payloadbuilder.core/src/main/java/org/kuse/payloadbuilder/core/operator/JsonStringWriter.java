package org.kuse.payloadbuilder.core.operator;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.Iterator;

import org.kuse.payloadbuilder.core.OutputWriter;

/** Writer that writes output as JSON string */
public class JsonStringWriter implements OutputWriter
{
    private StringBuilder sb = new StringBuilder();

    /** Append string */
    public void append(String string)
    {
        sb.append(string);
    }

    @Override
    public void endRow()
    {
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ',')
        {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append(System.lineSeparator());
    }

    /** Return string and reset buffer */
    public String getAndReset()
    {
        if (sb.length() > 0 && sb.charAt(sb.length() - 1) == ',')
        {
            sb.deleteCharAt(sb.length() - 1);
        }
        String result = sb.toString();
        sb = new StringBuilder();
        return result;
    }

    @Override
    public void writeFieldName(String name)
    {
        sb.append("\"").append(name).append("\":");
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

        if (value instanceof String)
        {
            sb.append("\"");
        }
        if (value instanceof Float)
        {
            value = new BigDecimal((Float) value).setScale(2, RoundingMode.HALF_UP);
        }
        else if (value instanceof Double)
        {
            value = new BigDecimal((Double) value).setScale(2, RoundingMode.HALF_UP);
        }
        sb.append(value);
        if (value instanceof String)
        {
            sb.append("\"");
        }

        sb.append(",");
    }

    @Override
    public void startObject()
    {
        sb.append("{");
    }

    @Override
    public void endObject()
    {
        if (sb.charAt(sb.length() - 1) != '{')
        {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("}");
        sb.append(",");
    }

    @Override
    public void startArray()
    {
        sb.append("[");
    }

    @Override
    public void endArray()
    {
        if (sb.charAt(sb.length() - 1) != '[')
        {
            sb.deleteCharAt(sb.length() - 1);
        }
        sb.append("]");
        sb.append(",");
    }
}
