package se.kuseman.payloadbuilder.bytes.example;

import java.util.List;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;

/** Root entity - one row of the top level {@link TupleVector} produced by {@link se.kuseman.payloadbuilder.bytes.PayloadReader#readTupleVector}. */
final class Product implements IProduct
{
    private final ColumnCache columns;
    private final int row;

    Product(TupleVector tupleVector, int row)
    {
        this.columns = new ColumnCache(tupleVector, ProductSchema.COLUMN_COUNT);
        this.row = row;
    }

    @Override
    public int getId()
    {
        return columns.column(ProductSchema.ID)
                .getInt(row);
    }

    @Override
    public UTF8String getName()
    {
        // No toString() here on purpose - see IProduct#getName's javadoc
        return columns.column(ProductSchema.NAME)
                .getString(row);
    }

    @Override
    public Decimal getPrice()
    {
        return columns.column(ProductSchema.PRICE)
                .getDecimal(row);
    }

    @Override
    public List<ISku> getSkus()
    {
        // Lazy: TableVector#getTable(row) only decodes this product's sku table because we asked for it here
        return new SkuList(columns.column(ProductSchema.SKUS)
                .getTable(row));
    }
}
