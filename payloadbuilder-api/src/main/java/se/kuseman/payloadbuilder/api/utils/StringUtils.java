package se.kuseman.payloadbuilder.api.utils;

/** String utils */
public class StringUtils
{
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
}
