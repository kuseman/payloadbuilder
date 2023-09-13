package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Factory that creates {@link VectorWriter}'s */
class VectorFactory
{
    private VectorFactory()
    {
    }

    /** Get value vector from type and buffer. Buffer must be positioned at vector data start */
    static ValueVector getVector(ByteBuffer buffer, int position, ReadContext context, ResolvedType resolvedType)
    {
        /*
         * @formatter:off
         * Vector 
         * - type bytes (see Utils#writeResolvedType) (only on root)
         * - length (varInt)
         * - nullLength (varInt)
         * - nullByte0
         * - nullByte1
         * - nullByteX
         * - vector implementation version
         * - data
         * @formatter:on
         */

        ResolvedType type = resolvedType;
        // Resolved type is null, then we are on root, read the full type from payload
        if (resolvedType == null)
        {
            Schema schema = context.getSchema();

            // There is a provided schema, then don't resolve the type but verify it against provided schema
            if (schema != null)
            {
                if (context.isExpandSchema()
                        || schema.getSize() == 0)
                {
                    Reference<ResolvedType> ref = new Reference<>();
                    position = Utils.expandType(buffer, position, ResolvedType.table(schema), ref);
                    type = ref.getValue();
                }
                else
                {
                    type = ResolvedType.table(schema);
                    position = Utils.validateResolvedType(buffer, position, type);
                }
            }
            else
            {
                Reference<ResolvedType> ref = new Reference<>();
                position = Utils.readResolvedType(buffer, position, ref);
                type = ref.getValue();
            }
        }

        int length = Utils.readVarInt(buffer, position);
        position += Utils.sizeOfVarInt(length);

        Reference<NullBuffer> ref = new Reference<>();
        position = NullBuffer.getNullBuffer(buffer, position, length, ref);

        NullBuffer nullBuffer = ref.getValue();

        if (nullBuffer.isAllNull())
        {
            return ValueVector.literalNull(type, length);
        }

        byte version = buffer.get(position++);

        switch (type.getType())
        {
            case Boolean:
                return BooleanVector.getVector(buffer, position, nullBuffer, version, length);
            case Int:
                return IntVector.getVector(buffer, position, nullBuffer, version, length);
            case Long:
                return LongVector.getVector(buffer, position, nullBuffer, version, length);
            case Float:
                return FloatVector.getVector(buffer, position, nullBuffer, version, length);
            case Double:
                return DoubleVector.getVector(buffer, position, nullBuffer, version, length);
            case Table:
                return TableVector.getVector(buffer, position, context, nullBuffer, type, version, length);
            case String:
                return StringVector.getVector(buffer, position, nullBuffer, version, length);
            case DateTime:
                return DateTimeVector.getVector(buffer, position, nullBuffer, version, length);
            case Decimal:
                return DecimalVector.getVector(buffer, position, context, nullBuffer, version, length);
            case Object:
                return ObjectVector.getVector(buffer, position, context, nullBuffer, type, version, length);
            case Array:
                return ArrayVector.getVector(buffer, position, context, nullBuffer, type, version, length);
            default:
                throw new IllegalArgumentException("Vectors of type: " + type + " is not supported");

        }
    }
}
