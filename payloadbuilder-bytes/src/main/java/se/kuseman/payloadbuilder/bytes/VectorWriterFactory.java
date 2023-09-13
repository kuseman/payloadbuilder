package se.kuseman.payloadbuilder.bytes;

import se.kuseman.payloadbuilder.api.catalog.Column;

/** Factory that creates {@link VectorWriter}'s */
class VectorWriterFactory
{
    private VectorWriterFactory()
    {
    }

    static VectorWriter getWriter(Column.Type type)
    {
        switch (type)
        {
            case Boolean:
                return BooleanVectorWriter.INSTANCE;
            case Int:
                return IntVectorWriter.INSTANCE;
            case Long:
                return LongVectorWriter.INSTANCE;
            case Float:
                return FloatVectorWriter.INSTANCE;
            case Double:
                return DoubleVectorWriter.INSTANCE;
            case Table:
                return TableVectorWriter.INSTANCE;
            case String:
                return StringVectorWriter.INSTANCE;
            case DateTime:
                return DateTimeVectorWriter.INSTANCE;
            case Decimal:
                return DecimalVectorWriter.INSTANCE;
            case Object:
                return ObjectVectorWriter.INSTANCE;
            case Array:
                return ArrayVectorWriter.INSTANCE;
            default:
                throw new IllegalArgumentException("Writing vectors of type: " + type + " is not supported");

        }
    }
}
