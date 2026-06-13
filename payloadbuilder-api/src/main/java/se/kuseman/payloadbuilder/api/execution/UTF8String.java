package se.kuseman.payloadbuilder.api.execution;

import static java.util.Objects.requireNonNull;

import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;

/**
 * A string implementation that works directly on an under laying byte-array without realizing a java.lang.String. Equals/hash/comparision/concats etc. can be performed on byte level. Instance can be
 * created from both UTF8 bytes aswell as Latin1 (ISO_8859_1) encoded byte arrays. A java.lang.String can also be used to instantiate then all operations are performed via java.lang.String.
 */
public class UTF8String implements Comparable<UTF8String>, ValueVector, CharSequence
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

    private final boolean latin1;
    private String string;
    private int charLength = -1;
    private boolean hashIsZero;
    private int hash;
    private byte[] bytes;
    private int offset;
    private int byteLength;

    private UTF8String(String string)
    {
        requireNonNull(string);
        this.string = string;
        this.charLength = string.length();
        this.latin1 = false;
    }

    private UTF8String(byte[] bytes, int offset, int length, boolean latin1)
    {
        requireNonNull(bytes);
        this.bytes = requireNonNull(bytes, "bytes");
        this.offset = offset;
        this.byteLength = length;
        this.latin1 = latin1;
    }

    /** Return a copy of the underlying utf8 bytes for this string */
    public byte[] getBytes()
    {
        getBytesInternal();
        return Arrays.copyOfRange(bytes, offset, offset + byteLength);
    }

    /** Return the bytes of this instance into destination byte array. Caller is responsible for correct length */
    public void getBytes(byte[] destination)
    {
        getBytesInternal();
        System.arraycopy(this.bytes, offset, destination, 0, byteLength);
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
        return ResolvedType.STRING;
    }

    @Override
    public boolean hasNulls()
    {
        return false;
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

        // Same encoding => compare bytes
        if (latin1 == that.latin1)
        {
            // UTF8 can be compared lexicographically by unsigned byte comparison
            byte[] thisBytes = getBytesInternal();
            int thisOffset = this.offset;
            byte[] thatBytes = that.getBytesInternal();
            int thatOffset = that.offset;

            int size = thisOffset + Math.min(this.byteLength, that.byteLength);

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

            return this.byteLength - that.byteLength;
        }

        int length = length();
        int thatLength = that.length();
        // Else compare using charAt
        int size = Math.min(length, thatLength);
        for (int i = 0; i < size; i++)
        {
            char a = charAt(i);
            char b = that.charAt(i);
            int diff = a - b;
            if (diff != 0)
            {
                return diff;
            }
        }
        return length - thatLength;
    }

    // CharSequence

    @Override
    public int length()
    {
        if (charLength >= 0)
        {
            return charLength;
        }

        if (latin1)
        {
            charLength = byteLength;
            return charLength;
        }

        // Count UTF8 chars
        int count = 0;
        int i = offset;
        int end = offset + byteLength;

        while (i < end)
        {
            int b = bytes[i] & 0xFF;

            if (b < 0x80)
            {
                i += 1;
                count += 1;
            }
            else if ((b >> 5) == 0b110)
            {
                i += 2;
                count += 1;
            }
            else if ((b >> 4) == 0b1110)
            {
                i += 3;
                count += 1;
            }
            else if ((b >> 3) == 0b11110)
            {
                i += 4;
                count += 2;
            }
            else
            {
                i += 1;
                count += 1;
            }
        }

        charLength = count;
        return count;
    }

    @Override
    public char charAt(int index)
    {
        if (index < 0)
            throw new IndexOutOfBoundsException();

        if (string != null)
        {
            return string.charAt(index);
        }

        if (latin1)
        {
            if (index >= byteLength)
                throw new IndexOutOfBoundsException();
            return (char) (bytes[offset + index] & 0xFF);
        }

        // Find UTF8 char
        int i = offset;
        int end = offset + byteLength;
        int charPos = 0;

        while (i < end)
        {
            int b = bytes[i] & 0xFF;
            int codePoint;
            int byteCount;

            if (b < 0x80)
            {
                codePoint = b;
                byteCount = 1;
            }
            else if ((b >> 5) == 0b110)
            {
                codePoint = ((b & 0x1F) << 6) | (bytes[i + 1] & 0x3F);
                byteCount = 2;
            }
            else if ((b >> 4) == 0b1110)
            {
                codePoint = ((b & 0x0F) << 12) | ((bytes[i + 1] & 0x3F) << 6) | (bytes[i + 2] & 0x3F);
                byteCount = 3;
            }
            else if ((b >> 3) == 0b11110)
            {
                codePoint = ((b & 0x07) << 18) | ((bytes[i + 1] & 0x3F) << 12) | ((bytes[i + 2] & 0x3F) << 6) | (bytes[i + 3] & 0x3F);
                byteCount = 4;
            }
            else
            {
                codePoint = b;
                byteCount = 1;
            }

            if (codePoint <= 0xFFFF)
            {
                if (charPos == index)
                    return (char) codePoint;
                charPos++;
            }
            else
            {
                int cpPrime = codePoint - 0x10000;
                char high = (char) ((cpPrime >> 10) + 0xD800);
                char low = (char) ((cpPrime & 0x3FF) + 0xDC00);

                if (charPos == index)
                    return high;
                if (charPos + 1 == index)
                    return low;
                charPos += 2;
            }

            i += byteCount;
        }

        throw new IndexOutOfBoundsException();
    }

    @Override
    public CharSequence subSequence(int start, int end)
    {
        return toString().subSequence(start, end);
    }

    // End Of CharSequence

    @Override
    public int hashCode()
    {
        // NOTE! We must always use hash code from bytes, we cannot mix
        // hash code from string and bytes since then hash match don't work
        getBytesInternal();

        int result = hash;
        if (hash == 0
                && !hashIsZero)
        {
            result = START;
            int end = offset + byteLength;
            for (int i = offset; i < end; i++)
            {
                result = result * CONSTANT + bytes[i];
            }

            if (result == 0)
            {
                hashIsZero = true;
            }
            else
            {
                hash = result;
            }

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
        else if (obj instanceof UTF8String that)
        {
            if (string != null
                    && that.string != null)
            {
                return string.equals(that.string);
            }

            // Same encoding => compare bytes
            if (latin1 == that.latin1)
            {
                byte[] bytes1 = getBytesInternal();
                byte[] bytes2 = that.getBytesInternal();

                if (byteLength != that.byteLength)
                {
                    return false;
                }

                for (int i = 0; i < byteLength; i++)
                {
                    if (bytes1[offset + i] != bytes2[that.offset + i])
                    {
                        return false;
                    }
                }
            }
            // Else compare using charAt
            else
            {
                int length = this.length();
                if (length != that.length())
                {
                    return false;
                }

                for (int i = 0; i < length; i++)
                {
                    if (charAt(i) != that.charAt(i))
                    {
                        return false;
                    }
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

    /**
     * Returns true if this string is latin1 encoded. NOTE! Only applicable if {@link #hasString()} is false, then the string is encoded in whatever the java.lang.String is encoded with.
     */
    public boolean isLatin1()
    {
        return latin1;
    }

    /** Return the byte length of this instance. */
    public int getByteLength()
    {
        getBytesInternal();
        return byteLength;
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
            byteLength = bytes.length;
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

        if (latin1)
        {
            string = new String(bytes, offset, byteLength, StandardCharsets.ISO_8859_1);
        }
        else
        {
            string = new String(bytes, offset, byteLength, StandardCharsets.UTF_8);
        }
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
            System.arraycopy(str.getBytesInternal(), str.offset, bytes, offset, str.byteLength);
            offset += str.byteLength;
            // Don't add a last delimiter
            if (i < count - 1
                    && delimeter.byteLength > 0)
            {
                System.arraycopy(delimeter.getBytesInternal(), delimeter.offset, bytes, offset, delimeter.byteLength);
                offset += delimeter.byteLength;
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
        requireNonNull(object);
        if (object instanceof Boolean)
        {
            return ((Boolean) object).booleanValue() ? TRUE
                    : FALSE;
        }
        else if (object instanceof UTF8String utf8s)
        {
            return utf8s;
        }
        else if (object instanceof byte[] bytes)
        {
            boolean latin1 = detectLatin1(bytes, 0, bytes.length);
            return new UTF8String(bytes, 0, bytes.length, latin1);
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

    /** Constructs a string from utf8 bytes. */
    public static UTF8String utf8(byte[] bytes, int offset, int length)
    {
        return new UTF8String(bytes, offset, length, false);
    }

    public static UTF8String latin(byte[] bytes)
    {
        return latin(bytes, 0, bytes.length);
    }

    /**
     * Create a utf8 string from latin encoded bytes. NOTE! This does assume all bytes are latin1 encoded and does not check if that holds true.
     */
    public static UTF8String latin(byte[] bytes, int offset, int length)
    {
        return new UTF8String(bytes, offset, length, true);
    }

    /** Detects if provided bytes are all latin1 encoded. */
    public static boolean detectLatin1(byte[] bytes, int offset, int length)
    {
        int end = offset + length;
        int i = offset;

        while (i < end)
        {
            int b = bytes[i] & 0xFF;

            if (b < 0x80)
            {
                i++;
            }
            else if ((b >> 5) == 0b110
                    && i + 1 < end
                    && (bytes[i + 1] & 0xC0) == 0x80)
            {
                return false;
            }
            else if ((b >> 4) == 0b1110
                    && i + 2 < end
                    && (bytes[i + 1] & 0xC0) == 0x80
                    && (bytes[i + 2] & 0xC0) == 0x80)
            {
                return false;
            }
            else if ((b >> 3) == 0b11110
                    && i + 3 < end
                    && (bytes[i + 1] & 0xC0) == 0x80
                    && (bytes[i + 2] & 0xC0) == 0x80
                    && (bytes[i + 3] & 0xC0) == 0x80)
            {
                return false;
            }
            else
            {
                i++;
            }
        }
        return true;
    }
}
