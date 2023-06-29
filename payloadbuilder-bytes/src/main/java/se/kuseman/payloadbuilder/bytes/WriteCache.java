package se.kuseman.payloadbuilder.bytes;

import java.util.HashMap;
import java.util.Map;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Cache used during writing to reuse bytes from equals string/bigdecimals/longs etc. */
class WriteCache
{
    private Map<Long, Integer> longCache = new HashMap<>();
    private Map<Double, Integer> doubleCache = new HashMap<>();
    private Map<UTF8String, Integer> stringCache = new HashMap<>();
    private Map<Decimal, Integer> decimalCache = new HashMap<>();

    /** Get position to provided long value */
    Integer getLongPosition(long value)
    {
        return longCache.get(value);
    }

    /** Put a long position into cache */
    void putLongPosition(long value, int position)
    {
        longCache.put(value, position);
    }

    /** Get position to provided double value */
    Integer getDoublePosition(double value)
    {
        return doubleCache.get(value);
    }

    /** Put a double position into cache */
    void putDoublePosition(double value, int position)
    {
        doubleCache.put(value, position);
    }

    /** Get position to provided string value */
    Integer getStringPosition(UTF8String value)
    {
        return stringCache.get(value);
    }

    /** Put a string position into cache */
    void putStringPosition(UTF8String value, int position)
    {
        stringCache.put(value, position);
    }

    /** Get position to provided decimal value */
    Integer getDecimalPosition(Decimal value)
    {
        return decimalCache.get(value);
    }

    /** Put a string position into cache */
    void putDecimalPosition(Decimal value, int position)
    {
        decimalCache.put(value, position);
    }
}