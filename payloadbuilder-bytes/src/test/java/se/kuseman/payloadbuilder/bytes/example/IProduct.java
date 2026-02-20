package se.kuseman.payloadbuilder.bytes.example;

import java.util.List;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** A single product, with its list of skus. */
public interface IProduct
{
    int getId();

    /** See {@link ISku#getName()} for why this returns {@link UTF8String} rather than {@link String}. */
    UTF8String getName();

    Decimal getPrice();

    List<ISku> getSkus();
}
