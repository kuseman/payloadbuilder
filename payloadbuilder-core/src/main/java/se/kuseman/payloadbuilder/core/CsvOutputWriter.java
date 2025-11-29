package se.kuseman.payloadbuilder.core;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isEmpty;

import java.io.IOException;
import java.io.Reader;
import java.io.StringReader;
import java.io.StringWriter;
import java.io.Writer;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;

/** Output writer that writes CSV */
public class CsvOutputWriter implements OutputWriter
{
    private static final char CHAR_10 = 10;
    private static final char CHAR_13 = 13;
    private static final char[] NULL = "null".toCharArray();
    private static final char[] TRUE = "true".toCharArray();
    private static final char[] FALSE = "false".toCharArray();

    private Writer writer;
    private final CsvSettings settings;
    private boolean firstEntryOnRow = true;
    private boolean firstResultSet = true;
    private int objectNestCount;
    private String rowSeparator;
    private String resultSetSeparator;

    /** If headers is unknown when init, write values into buffer until first row is ready */
    private StringWriter buffer;
    private Writer temp;

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
        rowSeparator = null;
        resultSetSeparator = null;
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

    // CSOFF
    @Override
    // CSON
    public void initResult(String[] columns)
    {
        // Init separators to avoid creating strings for every row
        if (rowSeparator == null
                && !isEmpty(settings.rowSeparator))
        {
            rowSeparator = unescape(settings.rowSeparator);
        }
        if (resultSetSeparator == null
                && !isEmpty(settings.resultSetSeparator))
        {
            resultSetSeparator = unescape(settings.resultSetSeparator);
        }

        if (!firstResultSet)
        {
            newResultSet();
        }
        firstResultSet = false;

        if (!settings.writeHeaders)
        {
            return;
        }

        // Delayed header writing, switch buffers
        if (columns == null
                || columns.length == 0)
        {
            buffer = new StringWriter();
            temp = writer;
            writer = buffer;
            return;
        }

        if (Arrays.stream(columns)
                .anyMatch(StringUtils::isNotBlank))
        {
            startRow();

            for (String column : columns)
            {
                writeValue(column);
            }

            endRow();
        }
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

    private void writeValueInternal(Writer writer, Object value)
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
            throw new RuntimeException("Error writing CSV value", e);
        }
    }

    private void writeRowSeparator(Writer writer)
    {
        if (!isEmpty(rowSeparator))
        {
            try
            {
                writer.write(rowSeparator);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error ending CSV row", e);
            }
        }
    }

    private void write(Writer writer, Reader reader) throws IOException
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
                else if (settings.escapeNewLines
                        && (c == CHAR_10
                                || c == CHAR_13))
                {
                    writer.write(settings.escapeChar);
                    writer.write(c == CHAR_10 ? 'n'
                            : 'r');
                    continue;
                }
                writer.write(c);
            }
        }
    }

    private void newResultSet()
    {
        if (!isEmpty(resultSetSeparator))
        {
            try
            {
                writer.write(resultSetSeparator);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error writing CSV result set separator", e);
            }
        }
    }

    static String unescape(String separator)
    {
        return separator.replace("\\n\\r", "\n\r")
                .replace("\\n", "\n")
                .replace("\\r", "\r")
                .replace("\\t", "\t");
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

        private String rowSeparator = "\\n";
        private String resultSetSeparator = "\\n";

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

        /** Construct settings from a list of options. */
        public static CsvSettings fromOptions(IExecutionContext context, List<Option> options)
        {
            CsvSettings settings = new CsvSettings();

            String str;
            for (Option option : options)
            {
                if (QualifiedName.of("separatorChar")
                        .equalsIgnoreCase(option.getOption())
                        || QualifiedName.of("columnSeparator")
                                .equalsIgnoreCase(option.getOption()))
                {
                    str = option.getValueExpression()
                            .eval(context)
                            .valueAsString(0);
                    settings.separatorChar = str != null
                            && str.length() >= 1 ? str.charAt(0)
                                    : settings.separatorChar;
                }
                else if (QualifiedName.of("escapeChar")
                        .equalsIgnoreCase(option.getOption()))
                {
                    str = option.getValueExpression()
                            .eval(context)
                            .valueAsString(0);
                    settings.escapeChar = str != null
                            && str.length() >= 1 ? str.charAt(0)
                                    : settings.escapeChar;
                }
            }

            return settings;
        }
    }
}
