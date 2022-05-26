package se.kuseman.payloadbuilder.catalog.es;

import org.apache.commons.lang3.StringUtils;

/** Auth type */
public enum AuthType
{
    NONE,
    BASIC;

    static AuthType from(Object value)
    {
        if (value == null)
        {
            return NONE;
        }
        return AuthType.valueOf(StringUtils.upperCase(String.valueOf(value)));
    }
}