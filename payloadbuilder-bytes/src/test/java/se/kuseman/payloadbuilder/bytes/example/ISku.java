package se.kuseman.payloadbuilder.bytes.example;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** A single sku belonging to a {@link IProduct}. */
public interface ISku
{
    int getId();

    /**
     * Returns the name as {@link UTF8String} rather than {@link String} on purpose - for a payload backed value this is a zero-copy view over the payload's own backing byte array, no
     * decode/allocation happens until you ask for one (eg. via {@code toString()}, or ideally never, by writing its bytes straight out with {@link UTF8String#getBytes(byte[])} +
     * {@code JsonGenerator#writeUTF8String}). This is where this library's low-allocation story actually pays off - forcing a {@link String} here would throw that away for every caller, including the
     * ones that never needed one.
     */
    UTF8String getName();

    Decimal getPrice();
}
