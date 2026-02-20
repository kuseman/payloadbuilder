package se.kuseman.payloadbuilder.bytes.example;

import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.bytes.PayloadReader;
import se.kuseman.payloadbuilder.bytes.SchemaReconciliationCache;

/** Entry point for reading a product payload back into a typed {@link IProduct}. */
public final class Products
{
    /**
     * Shared across every schema/table this application reads, for the application's whole lifetime - see {@link SchemaReconciliationCache}'s javadoc for why a single instance is enough.
     */
    private static final SchemaReconciliationCache RECONCILIATION_CACHE = new SchemaReconciliationCache(64);

    private Products()
    {
    }

    public static IProduct fromBytes(byte[] bytes)
    {
        TupleVector tupleVector = PayloadReader.readTupleVector(bytes, ProductSchema.SCHEMA, false, RECONCILIATION_CACHE);
        return new Product(tupleVector, 0);
    }
}
