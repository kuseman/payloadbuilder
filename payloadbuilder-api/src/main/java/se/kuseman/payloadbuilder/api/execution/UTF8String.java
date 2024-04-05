package se.kuseman.payloadbuilder.api.execution;

import static java.util.Objects.requireNonNull;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;

/**
 * A bytes reference used for data types that supports operations directly on under laying byte structures like Strings etc. NOTE! {@link ValueVector} is implemented here to let a single string become
 * a literal value vector of it self to avoid creating a literal
 */
public class UTF8String implements Comparable<UTF8String>, ValueVector
{
    private static final ThreadLocal<StringBuilder> BUILDER = new ThreadLocal<>();

    public static final UTF8String EMPTY = UTF8String.from("")
            .loadBytes();
    public static final UTF8String COMMA = UTF8String.from(",")
            .loadBytes();
    private static final UTF8String TRUE = UTF8String.from("true")
            .loadBytes();
    private static final UTF8String FALSE = UTF8String.from("false")
            .loadBytes();

    private static final int START = 17;
    private static final int CONSTANT = 37;

    private String string;

    private byte[] bytes;
    private int offset;
    private int length;

    private UTF8String(String string)
    {
        this.string = string;
    }

    private UTF8String(byte[] bytes, int offset, int length)
    {
        this.bytes = requireNonNull(bytes, "bytes");
        this.offset = offset;
        this.length = length;
    }

    /** Return a copy of the underlying utf8 bytes for this string */
    public byte[] getBytes()
    {
        getBytesInternal();
        return Arrays.copyOfRange(bytes, offset, offset + length);
    }

    /** Return the bytes of this instance into destination byte array. Caller is responsible for correct length */
    public void getBytes(byte[] destination)
    {
        getBytesInternal();
        System.arraycopy(this.bytes, offset, destination, 0, length);
    }

    // ValueVector

    @Override
    public int size()
    {
        return 1;
    }

    @Override
    public ResolvedType type()
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public boolean isNull(int row)
    {
        return false;
    }

    @Override
    public UTF8String getString(int row)
    {
        return this;
    }

    // End ValueVector

    /** Compare this reference to another bytes reference */
    @Override
    public int compareTo(UTF8String that)
    {
        if (this == that)
        {
            return 0;
        }

        if (string != null
                && that.string != null)
        {
            return string.compareTo(that.string);
        }

        // UTF8 can be compared lexicographically by unsigned byte comparison
        byte[] thisBytes = getBytesInternal();
        int thisOffset = this.offset;
        byte[] thatBytes = that.getBytesInternal();
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

    @Override
    public int hashCode()
    {
        if (string != null)
        {
            return string.hashCode();
        }

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
            if (string != null
                    && that.string != null)
            {
                return string.equals(that.string);
            }

            byte[] bytes1 = getBytesInternal();
            byte[] bytes2 = that.getBytesInternal();

            if (length != that.length)
            {
                return false;
            }

            for (int i = 0; i < length; i++)
            {
                if (bytes1[offset + i] != bytes2[that.offset + i])
                {
                    return false;
                }
            }
            return true;
        }
        return false;
    }

    /** Returns true if this instance has a {@link java.lang.String} instance. */
    public boolean hasString()
    {
        return string != null;
    }

    /** Return the byte length of this instance. */
    public int getByteLength()
    {
        getBytesInternal();
        return length;
    }

    private byte[] getBytesInternal()
    {
        if (bytes != null)
        {
            return bytes;
        }
        else if (string != null)
        {
            bytes = string.getBytes(StandardCharsets.UTF_8);
            length = bytes.length;
            offset = 0;
        }
        return bytes;
    }

    private UTF8String loadBytes()
    {
        getBytesInternal();
        return this;
    }

    // TODO: More string operations can be done here when codePoints are implemented
    // startsWith, endsWith, replace, subString

    /** Return a string representation of this instance */
    @Override
    public String toString()
    {
        if (string != null)
        {
            return string;
        }

        string = new String(bytes, offset, length, StandardCharsets.UTF_8);
        return string;
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

        int count = strings.size();

        // If all have java String then we should not use byte concat
        boolean allHaveStrings = delimeter.string != null;
        for (int i = 0; i < count; i++)
        {
            if (strings.get(i).string == null)
            {
                allHaveStrings = false;
                break;
            }
        }

        if (allHaveStrings)
        {
            return concatStringBuilder(delimeter, strings);
        }

        int size = 0;
        for (int i = 0; i < count; i++)
        {
            size += strings.get(i)
                    .getByteLength();
        }
        size += (count - 1) * delimeter.getByteLength();

        byte[] bytes = new byte[size];

        int offset = 0;
        for (int i = 0; i < count; i++)
        {
            UTF8String str = strings.get(i);
            System.arraycopy(str.getBytesInternal(), str.offset, bytes, offset, str.length);
            offset += str.length;
            // Don't add a last delimiter
            if (i < count - 1
                    && delimeter.length > 0)
            {
                System.arraycopy(delimeter.getBytesInternal(), delimeter.offset, bytes, offset, delimeter.length);
                offset += delimeter.length;
            }
        }

        return utf8(bytes);
    }

    private static UTF8String concatStringBuilder(UTF8String delimeter, List<UTF8String> strings)
    {
        int count = strings.size();
        StringBuilder sb = getBuilder();
        for (int i = 0; i < count; i++)
        {
            sb.append(strings.get(i).string);
            if (i < count - 1)
            {
                sb.append(delimeter.string);
            }
        }
        return new UTF8String(sb.toString());
    }

    private static StringBuilder getBuilder()
    {
        StringBuilder sb = BUILDER.get();
        if (sb == null)
        {
            sb = new StringBuilder(1024);
            BUILDER.set(sb);
        }
        sb.setLength(0);
        return sb;
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
        else if (object instanceof UTF8String)
        {
            return (UTF8String) object;
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
        return new UTF8String(string);
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
