package se.kuseman.payloadbuilder.api.utils;

import java.util.Iterator;
import java.util.Objects;

/** String utils */
public class StringUtils
{
    private static final int STRING_BUILDER_SIZE = 256;
    private static final String EMPTY = "";

    /** Returns true if provided sequence is blank */
    public static boolean isBlank(CharSequence seq)
    {
        if (seq == null)
        {
            return true;
        }
        final int strLen = seq.length();
        if (strLen == 0)
        {
            return true;
        }
        for (int i = 0; i < strLen; i++)
        {
            if (!Character.isWhitespace(seq.charAt(i)))
            {
                return false;
            }
        }
        return true;
    }

    /** Checks that provided string is not blank, throws exception otherwise */
    public static String requireNonBlank(String string, String message)
    {
        if (string == null
                || "".equals(string))
        {
            throw new IllegalArgumentException(message);
        }
        return string;
    }

    /** Repeat provided char length times */
    public static String repeat(char ch, int length)
    {
        StringBuilder sb = new StringBuilder(length);
        for (int i = 0; i < length; i++)
        {
            sb.append(ch);
        }
        return sb.toString();
    }

    /** Apache commons lang3 */
    public static String join(final Iterable<?> iterable, final char separator)
    {
        if (iterable == null)
        {
            return null;
        }
        return join(iterable.iterator(), separator);
    }

    /** Apache commons lang3 */
    public static String join(final Iterator<?> iterator, final char separator)
    {

        // handle null, zero and one elements before building a buffer
        if (iterator == null)
        {
            return null;
        }
        if (!iterator.hasNext())
        {
            return EMPTY;
        }
        final Object first = iterator.next();
        if (!iterator.hasNext())
        {
            return Objects.toString(first, EMPTY);
        }

        // two or more elements
        final StringBuilder buf = new StringBuilder(STRING_BUILDER_SIZE); // Java default is 16, probably too small
        if (first != null)
        {
            buf.append(first);
        }

        while (iterator.hasNext())
        {
            buf.append(separator);
            final Object obj = iterator.next();
            if (obj != null)
            {
                buf.append(obj);
            }
        }

        return buf.toString();
    }

    /**
     * Apache commons lang3
     */
    public static boolean equalsIgnoreCase(final CharSequence cs1, final CharSequence cs2)
    {
        if (cs1 == cs2)
        {
            return true;
        }
        if (cs1 == null
                || cs2 == null)
        {
            return false;
        }
        if (cs1.length() != cs2.length())
        {
            return false;
        }
        return regionMatches(cs1, true, 0, cs2, 0, cs1.length());
    }

    /**
     * Apache commons lang3
     */
    private static boolean regionMatches(final CharSequence cs, final boolean ignoreCase, final int thisStart, final CharSequence substring, final int start, final int length)
    {
        if (cs instanceof String
                && substring instanceof String)
        {
            return ((String) cs).regionMatches(ignoreCase, thisStart, (String) substring, start, length);
        }
        int index1 = thisStart;
        int index2 = start;
        int tmpLen = length;

        // Extract these first so we detect NPEs the same as the java.lang.String version
        final int srcLen = cs.length() - thisStart;
        final int otherLen = substring.length() - start;

        // Check for invalid parameters
        if (thisStart < 0
                || start < 0
                || length < 0)
        {
            return false;
        }

        // Check that the regions are long enough
        if (srcLen < length
                || otherLen < length)
        {
            return false;
        }

        while (tmpLen-- > 0)
        {
            final char c1 = cs.charAt(index1++);
            final char c2 = substring.charAt(index2++);

            if (c1 == c2)
            {
                continue;
            }

            if (!ignoreCase)
            {
                return false;
            }

            // The real same check as in String.regionMatches():
            final char u1 = Character.toUpperCase(c1);
            final char u2 = Character.toUpperCase(c2);
            if (u1 != u2
                    && Character.toLowerCase(u1) != Character.toLowerCase(u2))
            {
                return false;
            }
        }

        return true;
    }
}
