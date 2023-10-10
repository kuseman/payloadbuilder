package se.kuseman.payloadbuilder.core;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;

import org.apache.commons.io.IOUtils;

import se.kuseman.payloadbuilder.api.OutputWriter;

/** Output writer that writes plain text */
public class PlainTextOutputWriter implements OutputWriter
{
    private static final String ROW_SEPARATOR = "\r\n";
    private static final char[] NULL = "null".toCharArray();
    private static final char[] TRUE = "true".toCharArray();
    private static final char[] FALSE = "false".toCharArray();

    private Writer writer;
    private boolean firstEntryOnRow = true;
    private boolean firstResultSet = true;
    private int objectNestCount;

    /** If headers is unknown when init, write values into buffer until first row is ready */
    private StringWriter buffer;
    private Writer temp;

    public PlainTextOutputWriter(Writer writer)
    {
        this.writer = requireNonNull(writer, "writer");
        reset();
    }

    private void reset()
    {
        firstEntryOnRow = true;
        firstResultSet = true;
        objectNestCount = 0;
        buffer = null;
        temp = null;
    }

    @Override
    public void flush()
    {
        try
        {
            writer.flush();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error flushing stream", e);
        }
    }

    @Override
    public void close()
    {
        flush();
        try
        {
            writer.close();
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error closing stream", e);
        }
        // Reset state when done
        reset();
    }

    // CSOFF
    @Override
    // CSON
    public void initResult(String[] columns)
    {
        if (!firstResultSet)
        {
            newResultSet();
        }
        firstResultSet = false;

        // Delayed header writing, switch buffers
        if (columns == null
                || columns.length == 0)
        {
            buffer = new StringWriter();
            temp = writer;
            writer = buffer;
            return;
        }

        startRow();

        for (String column : columns)
        {
            writeValue(column);
        }

        endRow();
    }

    @Override
    public void startRow()
    {
        firstEntryOnRow = true;
    }

    @Override
    public void endRow()
    {
        writeRowSeparator(writer);

        // Delayed write
        // Now the buffer contains the first rows values
        // Temp contains the headers
        if (buffer != null)
        {
            // End header row with separator
            writeRowSeparator(temp);

            // Write the buffer (ie. first row values) to output
            // Skip leading separator char
            try
            {
                IOUtils.copy(new StringReader(buffer.getBuffer()
                        .substring(1)), temp);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error writing bufferd CSV data", e);
            }

            // Switch to normal writing
            writer = temp;
            temp = null;
            buffer = null;
        }
    }

    @Override
    public void writeFieldName(String name)
    {
        // Buffered write, then write the header to temp which is the real output writer
        if (buffer != null)
        {
            writeValueInternal(temp, name);
        }
    }

    @Override
    public void writeValue(Object value)
    {
        writeValueInternal(writer, value);
    }

    @Override
    public void startObject()
    {
        // Omit the first object on each row
        if (objectNestCount > 0)
        {
            writeValue("{");
        }
        objectNestCount++;
        firstEntryOnRow = true;
    }

    @Override
    public void endObject()
    {
        firstEntryOnRow = true;
        objectNestCount--;
        if (objectNestCount > 0)
        {
            writeValue("}");
        }
    }

    @Override
    public void startArray()
    {
        writeValue("[");
        firstEntryOnRow = true;
    }

    @Override
    public void endArray()
    {
        firstEntryOnRow = true;
        writeValue("]");
    }

    private void writeValueInternal(Writer writer, Object value)
    {
        try
        {
            if (!firstEntryOnRow)
            {
                writer.write(' ');
            }

            if (value == null)
            {
                writer.write(NULL);
            }
            else if (value instanceof Boolean)
            {
                char[] chars = ((Boolean) value).booleanValue() ? TRUE
                        : FALSE;
                writer.write(chars);
            }
            else if (value instanceof Reader)
            {
                write(writer, (Reader) value);
            }
            else
            {
                // ToString the value as fallback
                String str = String.valueOf(value);
                write(writer, new StringReader(str));
            }

            firstEntryOnRow = false;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing value", e);
        }
    }

    private void writeRowSeparator(Writer writer)
    {
        try
        {
            writer.write(ROW_SEPARATOR);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error ending row", e);
        }
    }

    private void write(Writer writer, Reader reader) throws IOException
    {
        try (Reader r = reader)
        {
            int c;
            while ((c = r.read()) != -1)
            {
                writer.write(c);
            }
        }
    }

    private void newResultSet()
    {
        try
        {
            writer.write(ROW_SEPARATOR);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing result set separator", e);
        }
    }
}
