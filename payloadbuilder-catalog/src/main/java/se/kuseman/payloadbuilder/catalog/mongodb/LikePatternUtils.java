package se.kuseman.payloadbuilder.catalog.mongodb;

import java.util.regex.Pattern;

/** Translates SQL {@code LIKE} patterns into Java regular expressions usable with Mongo's {@code $regex}. */
final class LikePatternUtils
{
    private LikePatternUtils()
    {
    }

    /** Converts provided SQL LIKE pattern (with {@code %}/{@code _} wildcards and an optional escape character) into an anchored regex. */
    static String toRegex(String pattern, Character escape)
    {
        StringBuilder sb = new StringBuilder("^");
        int length = pattern.length();
        for (int i = 0; i < length; i++)
        {
            char c = pattern.charAt(i);
            if (escape != null
                    && c == escape
                    && i + 1 < length)
            {
                char next = pattern.charAt(++i);
                sb.append(Pattern.quote(String.valueOf(next)));
            }
            else if (c == '%')
            {
                sb.append(".*");
            }
            else if (c == '_')
            {
                sb.append('.');
            }
            else
            {
                sb.append(Pattern.quote(String.valueOf(c)));
            }
        }
        sb.append("$");
        return sb.toString();
    }
}
