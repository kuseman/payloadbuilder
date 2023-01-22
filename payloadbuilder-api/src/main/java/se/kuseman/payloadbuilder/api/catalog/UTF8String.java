package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.List;

/**
 * A bytes reference used for data types that supports operations directly on under laying byte structures like Strings etc.
 */
public class UTF8String implements Comparable<UTF8String>
{
    public static final UTF8String EMPTY = UTF8String.from("");
    public static final UTF8String COMMA = UTF8String.from(",");
    private static final UTF8String TRUE = UTF8String.from("true");
    private static final UTF8String FALSE = UTF8String.from("false");

    private static final int START = 17;
    private static final int CONSTANT = 37;
    private final byte[] bytes;
    private final int offset;
    private final int length;

    private UTF8String(byte[] bytes, int offset, int length)
    {
        this.bytes = requireNonNull(bytes, "bytes");
        this.offset = offset;
        this.length = length;
    }

    @Override
    public int hashCode()
    {
        int result = START;
        final int end = offset + length;
        for (int i = offset; i < end; i++)
        {
            result = result * CONSTANT + bytes[i];
        }
        return result;
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof UTF8String)
        {
            UTF8String that = (UTF8String) obj;
            if (length != that.length)
            {
                return false;
            }
            for (int i = 0; i < length; i++)
            {
                if (bytes[offset + i] != that.bytes[that.offset + i])
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /** Compare this reference to another bytes reference */
    @Override
    public int compareTo(UTF8String that)
    {
        // UTF8 can be compared lexicographically by unsigned byte comparison
        if (this == that)
        {
            return 0;
        }

        byte[] thisBytes = this.bytes;
        int thisOffset = this.offset;
        byte[] thatBytes = that.bytes;
        int thatOffset = that.offset;

        int size = thisOffset + Math.min(this.length, that.length);

        while (thisOffset < size)
        {
            int a = thisBytes[thisOffset++] & 0xff;
            int b = thatBytes[thatOffset++] & 0xff;
            int diff = a - b;
            if (diff != 0)
            {
                return diff;
            }
        }

        return this.length - that.length;
    }

    /** Return the byte length of this instance. */
    public int byteLength()
    {
        return length;
    }

    // TODO: More string operations can be done here when codePoints are implemented
    // startsWith, endsWith, replace, subString

    /** Return a string representation of this instance */
    @Override
    public String toString()
    {
        return new String(bytes, offset, length, StandardCharsets.UTF_8);
    }

    /** Concats provided string with a comma delimiter */
    public static UTF8String concat(List<UTF8String> strings)
    {
        return concat(COMMA, strings);
    }

    /** Concats provided string with a delimiter */
    public static UTF8String concat(UTF8String delimeter, List<UTF8String> strings)
    {
        requireNonNull(strings);
        if (strings.isEmpty())
        {
            return EMPTY;
        }
        else if (strings.size() == 1)
        {
            return strings.get(0);
        }

        int size = 0;
        int count = strings.size();
        for (int i = 0; i < count; i++)
        {
            size += strings.get(i).length;
        }
        size += (count - 1) * delimeter.length;

        byte[] bytes = new byte[size];

        int offset = 0;
        for (int i = 0; i < count; i++)
        {
            UTF8String str = strings.get(i);
            System.arraycopy(str.bytes, str.offset, bytes, offset, str.length);
            offset += str.length;
            // Don't add a last delimiter
            if (i < count - 1)
            {
                System.arraycopy(delimeter.bytes, delimeter.offset, bytes, offset, delimeter.length);
                offset += delimeter.length;
            }
        }

        return utf8(bytes);
    }

    /**
     * Creates a string from provided object. Will fallback to toString if no known type is found
     */
    public static UTF8String from(Object object)
    {
        if (object instanceof Boolean)
        {
            return ((Boolean) object).booleanValue() ? TRUE
                    : FALSE;
        }
        return from(String.valueOf(object));
    }

    public static UTF8String from(boolean value)
    {
        return value ? TRUE
                : FALSE;
    }

    public static UTF8String from(String string)
    {
        return utf8(string.getBytes(StandardCharsets.UTF_8));
    }

    public static UTF8String utf8(byte[] bytes)
    {
        return utf8(bytes, 0, bytes.length);
    }

    public static UTF8String utf8(byte[] bytes, int offset, int length)
    {
        return new UTF8String(bytes, offset, length);
    }

    public static UTF8String latin(byte[] bytes)
    {
        return latin(bytes, 0, bytes.length);
    }

    /**
     * Create a utf8 string from latin encoded bytes. NOTE! Recommended usage is utf8 since this method allocates some when converting bytes.
     */
    public static UTF8String latin(byte[] bytes, int offset, int length)
    {
        ByteBuffer buffer = ByteBuffer.wrap(bytes, offset, length);
        CharBuffer charBuffer = StandardCharsets.ISO_8859_1.decode(buffer);
        ByteBuffer encoded = StandardCharsets.UTF_8.encode(charBuffer);
        return utf8(encoded.array(), 0, encoded.limit());
    }
}
