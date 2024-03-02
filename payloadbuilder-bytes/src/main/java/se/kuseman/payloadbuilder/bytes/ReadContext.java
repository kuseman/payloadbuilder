package se.kuseman.payloadbuilder.bytes;

import java.math.BigDecimal;
import java.util.HashMap;
import java.util.Map;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.catalog.Schema;

/** Cache used during reading of values. */
class ReadContext
{
    private final Schema schema;
    private final boolean expandSchema;
    private Map<Integer, BigDecimal> bigDecimalCache = new HashMap<>();

    ReadContext()
    {
        this(null, false);
    }

    ReadContext(Schema schema, boolean expandSchema)
    {
        this.schema = schema;
        this.expandSchema = expandSchema;
    }

    /** Get or compute big decimal */
    BigDecimal computeBigDecimal(int position, Supplier<BigDecimal> supplier)
    {
        return bigDecimalCache.computeIfAbsent(position, k -> supplier.get());
    }

    Schema getSchema()
    {
        return schema;
    }

    boolean isExpandSchema()
    {
        return expandSchema;
    }
}
