package org.kuse.payloadbuilder.core;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Json output writer */
public class JsonOutputWriter implements OutputWriter
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final JsonGenerator generator;
    private String currentField;

    public JsonOutputWriter(Writer writer)
    {
        try
        {
            this.generator = MAPPER.getFactory().createGenerator(requireNonNull(writer, "writer"));
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error creating JSON generator", e);
        }
    }

    @Override
    public void flush()
    {
        try
        {
            generator.flush();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error flushing JSON stream", e);
        }
    }

    @Override
    public void close()
    {
        flush();
        try
        {
            generator.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error closing CSV stream", e);
        }
    }

    @Override
    public void endRow()
    {
        try
        {
            generator.writeRaw(System.lineSeparator());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error ending JSON row", e);
        }
    }

    @Override
    public void writeFieldName(String name)
    {
        currentField = name;
    }

    @Override
    public void writeValue(Object value)
    {
        try
        {
            writeFieldName();
            if (value == null)
            {
                generator.writeNull();
            }
            else if (value instanceof Boolean)
            {
                generator.writeBoolean(((Boolean) value).booleanValue());
            }
            else if (value instanceof Double)
            {
                generator.writeNumber(((Double) value).doubleValue());
            }
            else if (value instanceof Float)
            {
                generator.writeNumber(((Float) value).floatValue());
            }
            else if (value instanceof Long)
            {
                generator.writeNumber(((Long) value).longValue());
            }
            else if (value instanceof Integer)
            {
                generator.writeNumber(((Integer) value).intValue());
            }
            else if (value instanceof Number)
            {
                generator.writeNumber(((Number) value).doubleValue());
            }
            else if (value instanceof Map)
            {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) value;

                startObject();
                for (Entry<String, Object> e : map.entrySet())
                {
                    writeFieldName(e.getKey());
                    writeValue(e.getValue());
                }
                endObject();
            }
            else if (value instanceof Collection)
            {
                currentField = null;
                @SuppressWarnings("unchecked")
                Collection<Object> col = (Collection<Object>) value;

                startArray();
                for (Object obj : col)
                {
                    writeValue(obj);
                }
                endArray();
            }
            else if (value instanceof Reader)
            {
                try (Reader reader = (Reader) value)
                {
                    generator.writeString(reader, -1);
                }
            }
            else
            {
                generator.writeString(String.valueOf(value));
            }
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing JSON value", e);
        }
    }

    @Override
    public void startObject()
    {
        try
        {
            writeFieldName();
            generator.writeStartObject();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing JSON start object", e);
        }
    }

    @Override
    public void endObject()
    {
        try
        {
            generator.writeEndObject();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing JSON end object", e);
        }
    }

    @Override
    public void startArray()
    {
        try
        {
            writeFieldName();
            generator.writeStartArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing JSON start array", e);
        }
    }

    @Override
    public void endArray()
    {
        try
        {
            generator.writeEndArray();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing JSON end array", e);
        }
    }

    private void writeFieldName() throws IOException
    {
        if (currentField == null)
        {
            return;
        }
        String field = currentField;
        currentField = null;
        generator.writeFieldName(field);
    }
}
