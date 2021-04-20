package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.Reader;
import java.nio.CharBuffer;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Replace */
class ReplaceFunction extends ScalarFunctionInfo
{
    ReplaceFunction(Catalog catalog)
    {
        super(catalog, "replace");
    }

    @Override
    public String getDescription()
    {
        return "Replaces all occurrences of specified string with a replacement string" + System.lineSeparator()
            + "Ex. replace(expression, searchExpression, replaceExpression)" + System.lineSeparator()
            + "NOTE! All input arguments is converted to String if not String already." + System.lineSeparator()
            + "      If any input evaluates to null, null is returned";
    }

    @Override
    public DataType getDataType(List<Expression> arguments)
    {
        return DataType.ANY;
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class, Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object arg0 = arguments.get(0).eval(context);
        Object arg1 = arguments.get(1).eval(context);
        Object arg2 = arguments.get(2).eval(context);
        if (arg0 == null
            || arg1 == null
            || arg2 == null)
        {
            return null;
        }

        String search = String.valueOf(arg1);
        String replacement = String.valueOf(arg2);
        if (arg0 instanceof Reader)
        {
            return new ReplacingReader((Reader) arg0, search, replacement);
        }

        String value = String.valueOf(arg0);
        return value.replace(search, replacement);
    }

    /**
     * Reader that replaces string on the while reading. This to avoid keeping whole string in memory
     */
    static class ReplacingReader extends Reader
    {
        private final Reader wrapped;
        private final String search;
        private final String replacement;
        private final char firstSearchChar;
        private final char firstReplacementChar;
        private final int searchLength;

        private StringBuilder buffer;
        private int bufferIndex;

        ReplacingReader(Reader wrapped, String search, String replacement)
        {
            this.wrapped = wrapped;
            this.search = requireNonNull(search, "search");
            this.replacement = requireNonNull(replacement, "replacement");
            this.firstSearchChar = search.charAt(0);
            this.firstReplacementChar = replacement.isEmpty() ? (char) 0 : replacement.charAt(0);
            this.searchLength = search.length();
        }

        @Override
        public int read() throws IOException
        {
            // Stream buffer
            if (buffer != null)
            {
                if (bufferIndex < buffer.length())
                {
                    return buffer.charAt(bufferIndex++);
                }
                else
                {
                    buffer = null;
                    bufferIndex = 0;
                }
            }

            int data = this.wrapped.read();
            if (data != firstSearchChar)
            {
                return data;
            }

            // First char is a match, start buffering
            // and see if we have a full search match
            // Since we know that we have a match on first char
            // we can skip that one in buffer
            buffer = new StringBuilder(searchLength - 1);
            for (int i = 1; i < searchLength; i++)
            {
                data = wrapped.read();
                if (data == -1)
                {
                    return firstSearchChar;
                }
                buffer.append((char) data);
                if (data != search.charAt(i))
                {
                    // No match, return first char in search
                    return firstSearchChar;
                }
            }

            if (replacement.isEmpty())
            {
                buffer = null;
                bufferIndex = 0;
                return wrapped.read();
            }

            // Match return replacement
            buffer = new StringBuilder(replacement);
            // Start at index 1 since first char is returned here
            bufferIndex = 1;
            return firstReplacementChar;
        }

        @Override
        public int read(char[] cbuf) throws IOException
        {
            return read(cbuf, 0, cbuf.length);
        }

        @Override
        public int read(char[] cbuf, int off, int len) throws IOException
        {
            int charsRead = 0;
            for (int i = 0; i < len; i++)
            {
                int nextChar = read();
                if (nextChar == -1)
                {
                    if (charsRead == 0)
                    {
                        charsRead = -1;
                    }
                    break;
                }
                charsRead = i + 1;
                cbuf[off + i] = (char) nextChar;
            }
            return charsRead;
        }

        @Override
        public void close() throws IOException
        {
            wrapped.close();
        }

        @Override
        public boolean ready() throws IOException
        {
            return wrapped.ready();
        }

        @Override
        public boolean markSupported()
        {
            return false;
        }

        @Override
        public int read(CharBuffer target) throws IOException
        {
            throw new RuntimeException("Operation Not Supported");
        }

        @Override
        public long skip(long n) throws IOException
        {
            throw new RuntimeException("Operation Not Supported");
        }

        @Override
        public void mark(int readAheadLimit) throws IOException
        {
            throw new RuntimeException("Operation Not Supported");
        }

        @Override
        public void reset() throws IOException
        {
            throw new RuntimeException("Operation Not Supported");
        }

        @Override
        public String toString()
        {
            try
            {
                return IOUtils.toString(wrapped).replace(search, replacement);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error replacing reader", e);
            }
        }
    }
}
