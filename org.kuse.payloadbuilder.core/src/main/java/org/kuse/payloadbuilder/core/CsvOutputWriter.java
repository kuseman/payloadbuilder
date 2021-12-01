package org.kuse.payloadbuilder.core;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.Writer;

/** Output writer that writes CSV */
public class CsvOutputWriter implements OutputWriter
{
    private static final char CHAR_10 = 10;
    private static final char CHAR_13 = 13;
    private static final char[] NULL = "null".toCharArray();
    private static final char[] TRUE = "true".toCharArray();
    private static final char[] FALSE = "false".toCharArray();

    private final Writer writer;
    private final CsvSettings settings;
    private boolean firstEntryOnRow = true;
    private boolean firstResultSet = true;
    private int objectNestCount;
    private boolean singleColumnResult;

    public CsvOutputWriter(Writer writer)
    {
        this(writer, new CsvSettings());
    }

    public CsvOutputWriter(Writer writer, CsvSettings settings)
    {
        this.writer = requireNonNull(writer, "writer");
        this.settings = requireNonNull(settings, "settings");
        reset();
    }

    private void reset()
    {
        firstEntryOnRow = true;
        firstResultSet = true;
        objectNestCount = 0;
        singleColumnResult = false;
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
            throw new RuntimeException("Error flushing CSV stream", e);
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
            throw new RuntimeException("Error closing CSV stream", e);
        }
        // Reset state when done
        reset();
    }

    @Override
    public void initResult(String[] columns)
    {
        if (!firstResultSet)
        {
            newResultSet();
        }
        firstResultSet = false;
        singleColumnResult = columns != null && columns.length == 1;
        // Omit headers if columns are unknown or there are a single column result or explicitly turned off
        if (columns == null
            || columns.length == 0
            || singleColumnResult
            || !settings.writeHeaders)
        {
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
        try
        {
            writer.write(System.lineSeparator());
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error ending CSV row", e);
        }
    }

    @Override
    public void writeFieldName(String name)
    {
        // Not used for CSV
    }

    @Override
    public void writeValue(Object value)
    {
        try
        {
            if (!firstEntryOnRow)
            {
                writer.write(settings.separatorChar);
            }

            if (value == null)
            {
                writer.write(NULL);
            }
            else if (value instanceof Boolean)
            {
                char[] chars = ((Boolean) value).booleanValue() ? TRUE : FALSE;
                writer.write(chars);
            }
            else if (value instanceof Reader)
            {
                write((Reader) value);
            }
            else
            {
                // ToString the value as fallback
                String str = String.valueOf(value);
                write(new StringReader(str));
            }

            firstEntryOnRow = false;
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error writing CSV value", e);
        }
    }

    @Override
    public void startObject()
    {
        // Omit the first object on each row
        if (objectNestCount > 0)
        {
            writeValue(settings.objectStartChar);
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
            writeValue(settings.objectEndChar);
        }
    }

    @Override
    public void startArray()
    {
        writeValue(settings.arrayStartChar);
        firstEntryOnRow = true;
    }

    @Override
    public void endArray()
    {
        firstEntryOnRow = true;
        writeValue(settings.arrayEndChar);
    }

    private void write(Reader reader) throws IOException
    {
        try (Reader r = reader)
        {
            int c;
            while ((c = r.read()) != -1)
            {
                if (c == settings.separatorChar)
                {
                    writer.write(settings.escapeChar);
                }
                else if (settings.escapeNewLines && (c == CHAR_10 || c == CHAR_13))
                {
                    writer.write(settings.escapeChar);
                    writer.write(c == CHAR_10 ? 'n' : 'r');
                    continue;
                }
                writer.write(c);
            }
        }
    }

    private void newResultSet()
    {
        if (!isBlank(settings.resultsetSeparator))
        {
            try
            {
                // Unescape known escapes
                writer.write(settings.resultsetSeparator
                        .replace("\\n\\r", System.lineSeparator())
                        .replace("\\n", "\n")
                        .replace("\\r", "\r")
                        .replace("\\t", "\t"));
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error writing CSV result set separator", e);
            }
        }
    }

    /** Csv writer settings */
    public static class CsvSettings
    {
        // TODO: quoting char

        private char escapeChar = '\\';
        private char separatorChar = ',';
        private char arrayStartChar = '[';
        private char arrayEndChar = ']';
        private char objectStartChar = '{';
        private char objectEndChar = '}';
        private boolean writeHeaders = true;
        private boolean escapeNewLines = true;
        private String resultsetSeparator = "\n";

        public char getEscapeChar()
        {
            return escapeChar;
        }

        public void setEscapeChar(char escapeChar)
        {
            this.escapeChar = escapeChar;
        }

        public char getSeparatorChar()
        {
            return separatorChar;
        }

        public void setSeparatorChar(char separatorChar)
        {
            this.separatorChar = separatorChar;
        }

        public char getArrayStartChar()
        {
            return arrayStartChar;
        }

        public void setArrayStartChar(char arrayStartChar)
        {
            this.arrayStartChar = arrayStartChar;
        }

        public char getArrayEndChar()
        {
            return arrayEndChar;
        }

        public void setArrayEndChar(char arrayEndChar)
        {
            this.arrayEndChar = arrayEndChar;
        }

        public char getObjectStartChar()
        {
            return objectStartChar;
        }

        public void setObjectStartChar(char objectStartChar)
        {
            this.objectStartChar = objectStartChar;
        }

        public char getObjectEndChar()
        {
            return objectEndChar;
        }

        public void setObjectEndChar(char objectEndChar)
        {
            this.objectEndChar = objectEndChar;
        }

        public boolean isWriteHeaders()
        {
            return writeHeaders;
        }

        public void setWriteHeaders(boolean writeHeaders)
        {
            this.writeHeaders = writeHeaders;
        }

        public boolean isEscapeNewLines()
        {
            return escapeNewLines;
        }

        public void setEscapeNewLines(boolean escapeNewLines)
        {
            this.escapeNewLines = escapeNewLines;
        }

        public String getResultsetSeparator()
        {
            return resultsetSeparator;
        }

        public void setResultsetSeparator(String resultsetSeparator)
        {
            this.resultsetSeparator = resultsetSeparator;
        }
    }
}
