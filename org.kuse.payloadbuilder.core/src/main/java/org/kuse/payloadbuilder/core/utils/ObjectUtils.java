package org.kuse.payloadbuilder.core.utils;

import org.apache.commons.lang3.StringUtils;

/** Object utils */
public final class ObjectUtils
{
    private ObjectUtils()
    {
    }

    /** Checks that provided string is not blank, throws exception otherwise */
    public static String requireNonBlank(String string, String message)
    {
        if (StringUtils.isBlank(string))
        {
            throw new IllegalArgumentException(message);
        }
        return string;
    }
}
