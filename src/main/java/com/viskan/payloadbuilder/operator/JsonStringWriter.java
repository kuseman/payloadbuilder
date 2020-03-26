package com.viskan.payloadbuilder.operator;

/** Writer that writes output as JSON string */
public class JsonStringWriter implements OutputWriter
{
    private StringBuilder sb = new StringBuilder();

    public String getAndReset()
    {
        if (sb.charAt(sb.length() - 1) == ',')
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
    public void writeValue(Object value)
    {
        if (value instanceof String)
        {
            sb.append("\"");
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
