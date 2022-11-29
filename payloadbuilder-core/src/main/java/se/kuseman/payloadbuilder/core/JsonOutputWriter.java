package se.kuseman.payloadbuilder.core;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.io.IOException;
import java.io.Reader;
import java.io.Writer;
import java.util.Collection;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.commons.io.input.CharSequenceReader;

import com.fasterxml.jackson.core.JsonGenerator;
import com.fasterxml.jackson.databind.ObjectMapper;

import se.kuseman.payloadbuilder.api.OutputWriter;

/** Json output writer */
public class JsonOutputWriter implements OutputWriter
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final JsonGenerator generator;
    private final JsonSettings settings;
    private String currentField;
    private boolean firstResultSet = true;

    private String rowSeparator;
    private String resultSetSeparator;

    public JsonOutputWriter(Writer writer)
    {
        this(writer, new JsonSettings());
    }

    public JsonOutputWriter(Writer writer, JsonSettings settings)
    {
        this.settings = settings;
        try
        {
            if (settings.prettyPrint)
            {
                this.generator = MAPPER.createGenerator(requireNonNull(writer, "writer"))
                        .useDefaultPrettyPrinter();
            }
            else
            {
                this.generator = MAPPER.getFactory()
                        .createGenerator(requireNonNull(writer, "writer"));
            }
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
        if (settings.allResultSetsAsOneArray)
        {
            endArray();
        }

        flush();
        try
        {
            generator.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error closing JSON stream", e);
        }

        // Reset
        firstResultSet = true;
        rowSeparator = null;
        resultSetSeparator = null;
    }

    @Override
    public void initResult(String[] columns)
    {
        // Init separators to avoid creating strings for every row
        if (rowSeparator == null
                && !isEmpty(settings.rowSeparator))
        {
            rowSeparator = CsvOutputWriter.unescape(settings.rowSeparator);
        }
        if (resultSetSeparator == null
                && !isEmpty(settings.resultSetSeparator))
        {
            resultSetSeparator = CsvOutputWriter.unescape(settings.resultSetSeparator);
        }

        if (firstResultSet)
        {
            if (settings.resultSetsAsArrays
                    || settings.allResultSetsAsOneArray)
            {
                startArray();
            }
        }
        else
        {
            if (settings.resultSetsAsArrays
                    && !settings.allResultSetsAsOneArray)
            {
                endArray();
            }

            if (!settings.allResultSetsAsOneArray
                    && !isEmpty(resultSetSeparator))
            {
                writeRaw(resultSetSeparator);
            }

            if (settings.resultSetsAsArrays
                    && !settings.allResultSetsAsOneArray)
            {
                startArray();
            }
        }

        firstResultSet = false;
    }

    @Override
    public void endRow()
    {
        if (!isEmpty(rowSeparator))
        {
            writeRaw(rowSeparator);
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
            writeFieldNameInternal();
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
            else if (value instanceof Iterator)
            {
                currentField = null;
                @SuppressWarnings("unchecked")
                Iterator<Object> it = (Iterator<Object>) value;

                startArray();
                while (it.hasNext())
                {
                    writeValue(it.next());
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
            else if (value instanceof CharSequence)
            {
                try (Reader reader = new CharSequenceReader((CharSequence) value))
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
    public void writeBool(boolean value)
    {
        try
        {
            generator.writeBoolean(value);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing JSON value", e);
        }
    }

    @Override
    public void writeInt(int value)
    {
        try
        {
            generator.writeNumber(value);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing JSON value", e);
        }
    }

    @Override
    public void writeLong(long value)
    {
        try
        {
            generator.writeNumber(value);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing JSON value", e);
        }
    }

    @Override
    public void writeFloat(float value)
    {
        try
        {
            generator.writeNumber(value);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing JSON value", e);
        }
    }

    @Override
    public void writeDouble(double value)
    {
        try
        {
            generator.writeNumber(value);
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
            writeFieldNameInternal();
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
            writeFieldNameInternal();
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

    private void writeFieldNameInternal() throws IOException
    {
        if (currentField == null)
        {
            return;
        }
        String field = currentField;
        currentField = null;
        generator.writeFieldName(field);
    }

    private void writeRaw(String value)
    {
        try
        {
            generator.writeRaw(value);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error ending JSON row", e);
        }
    }

    /** Json settings */
    public static class JsonSettings
    {
        private String rowSeparator = "";
        private String resultSetSeparator = "";

        /** Pretty print result */
        private boolean prettyPrint;

        /** Wrap result set rows as an array */
        private boolean resultSetsAsArrays;

        /** Wrap all result sets rows as an array. Mutual exclusive with {@link #resultSetsAsArrays} */
        private boolean allResultSetsAsOneArray;

        public String getRowSeparator()
        {
            return rowSeparator;
        }

        public void setRowSeparator(String rowSeparator)
        {
            this.rowSeparator = rowSeparator;
        }

        public String getResultSetSeparator()
        {
            return resultSetSeparator;
        }

        public void setResultSetSeparator(String resultSetSeparator)
        {
            this.resultSetSeparator = resultSetSeparator;
        }

        public boolean isResultSetsAsArrays()
        {
            return resultSetsAsArrays;
        }

        public void setResultSetsAsArrays(boolean resultSetsAsArrays)
        {
            this.resultSetsAsArrays = resultSetsAsArrays;
        }

        public boolean isAllResultSetsAsOneArray()
        {
            return allResultSetsAsOneArray;
        }

        public void setAllResultSetsAsOneArray(boolean allResultSetsAsOneArray)
        {
            this.allResultSetsAsOneArray = allResultSetsAsOneArray;
        }

        public boolean isPrettyPrint()
        {
            return prettyPrint;
        }

        public void setPrettyPrint(boolean prettyPrint)
        {
            this.prettyPrint = prettyPrint;
        }
    }
}
