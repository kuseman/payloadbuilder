package se.kuseman.payloadbuilder.bytes.example;

import se.kuseman.payloadbuilder.api.execution.Decimal;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Cheap, lazy row-wrapper - construction touches no data at all, each getter decodes only the column it needs. */
final class Sku implements ISku
{
    private final ColumnCache columns;
    private final int row;

    Sku(ColumnCache columns, int row)
    {
        this.columns = columns;
        this.row = row;
    }

    @Override
    public int getId()
    {
        // Assumed never null - a real project would document/enforce this at the write side
        return columns.column(SkuSchema.ID)
                .getInt(row);
    }

    @Override
    public UTF8String getName()
    {
        // No toString() here on purpose - see ISku#getName's javadoc
        ValueVector v = columns.column(SkuSchema.NAME);
        return v.isNull(row) ? null
                : v.getString(row);
    }

    @Override
    public Decimal getPrice()
    {
        ValueVector v = columns.column(SkuSchema.PRICE);
        return v.isNull(row) ? null
                : v.getDecimal(row);
    }
}
