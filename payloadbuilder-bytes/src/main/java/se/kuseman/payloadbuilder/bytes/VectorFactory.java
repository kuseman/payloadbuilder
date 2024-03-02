package se.kuseman.payloadbuilder.bytes;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
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
        ResolvedType payloadType = null;
        // Resolved type is null, then we are on root, read the full type from payload
        if (resolvedType == null)
        {
            Schema schema = context.getSchema();

            // There is a provided schema, then don't resolve the type but verify it against provided schema
            if (schema != null)
            {
                // Store position before we validate to be able to reset if type mismatches
                int originalPosition = position;

                BooleanReference typeMismatch = new BooleanReference(false);
                if (context.isExpandSchema()
                        || schema.getSize() == 0)
                {
                    Reference<ResolvedType> ref = new Reference<>();
                    position = Utils.expandType(buffer, position, ResolvedType.table(schema), ref, typeMismatch);
                    type = ref.getValue();
                }
                else
                {
                    type = ResolvedType.table(schema);
                    position = Utils.validateResolvedType(buffer, position, type, typeMismatch);
                }

                // We have a mismatch between expected schema and payloads
                // Recreate the type so that we have the payloads schema as actual
                // to be consistent with the payload. This is used later on when fetching columns
                // to actually fetch what's in the payload and let implicit casting resolve any issues
                // This is typical the case when upgrading clients where we have changed type on non used
                // columns. This is safe only if faulty columns are not accessed or can be implicitly casted.
                // However best practice is to only be in this state during a migration phase to avoid
                // recreating the schema on every read.
                if (typeMismatch.getValue())
                {
                    buffer.position(originalPosition);
                    Reference<ResolvedType> ref = new Reference<>();
                    position = Utils.readResolvedType(buffer, originalPosition, ref);
                    payloadType = ResolvedType.table(recreateSchema(schema, ref.getValue()
                            .getSchema(), context.isExpandSchema()));
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

        Reference<NullBuffer> nullBufferRef = new Reference<>();
        position = NullBuffer.getNullBuffer(buffer, position, length, nullBufferRef);

        NullBuffer nullBuffer = nullBufferRef.getValue();

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
                return TableVector.getVector(buffer, position, context, nullBuffer, type, payloadType, version, length);
            case String:
                return StringVector.getVector(buffer, position, nullBuffer, version, length);
            case DateTime:
                return DateTimeVector.getVector(buffer, position, nullBuffer, version, length);
            case Decimal:
                return DecimalVector.getVector(buffer, position, context, nullBuffer, version, length);
            case Object:
                return ObjectVector.getVector(buffer, position, context, nullBuffer, type, payloadType, version, length);
            case Array:
                return ArrayVector.getVector(buffer, position, context, nullBuffer, type, version, length);
            default:
                throw new IllegalArgumentException("Vectors of type: " + type + " is not supported");
        }
    }

    private static Schema recreateSchema(Schema expected, Schema payload, boolean expand)
    {
        int size = expand ? Math.max(expected.getSize(), payload.getSize())
                : expected.getSize();
        List<Column> columns = new ArrayList<>(size);
        for (int i = 0; i < size; i++)
        {
            Column expectedColumn = i < expected.getSize() ? expected.getColumns()
                    .get(i)
                    : null;
            Column payloadColumn = i < payload.getSize() ? payload.getColumns()
                    .get(i)
                    : null;

            // Expansion of expected schema
            if (expectedColumn == null)
            {
                columns.add(payloadColumn);
            }
            // New columns in expected that does not exists in payload, will be null's
            else if (payloadColumn == null)
            {
                columns.add(expectedColumn);
            }
            else
            {
                ResolvedType expectedResolvedType = expectedColumn.getType();
                ResolvedType payloadResolvedType = payloadColumn.getType();

                Column.Type expectedType = expectedResolvedType.getType();
                // Mismatch types => pick name from expected and type from payload
                if (expectedType != payloadColumn.getType()
                        .getType())
                {
                    columns.add(new Column(expectedColumn.getName(), payloadColumn.getType()));
                }
                // Recurse into sub schema
                else if (expectedType == Column.Type.Table
                        || expectedType == Column.Type.Object)
                {
                    Schema subSchema = recreateSchema(expectedColumn.getType()
                            .getSchema(),
                            payloadColumn.getType()
                                    .getSchema(),
                            expand);
                    columns.add(new Column(expectedColumn.getName(), expectedType == Column.Type.Table ? ResolvedType.table(subSchema)
                            : ResolvedType.object(subSchema)));
                }
                else if (expectedType == Column.Type.Array)
                {
                    // expected: Array<Array<Int>>
                    // payload: Array<Array<String>>

                    // expected: Array<Array<Object[int, double]>>
                    // payload: Array<Array<Object[int, boolean]>>

                    // Dig down until types differs, if that type is a Table or Object => recurse
                    // else add the expected name + payload type

                    int nestCount = 0;
                    Column.Type payloadType = payloadResolvedType.getType();
                    while (expectedType == Column.Type.Array
                            && payloadType == Column.Type.Array)
                    {
                        expectedResolvedType = expectedResolvedType.getSubType();
                        payloadResolvedType = payloadResolvedType.getSubType();

                        expectedType = expectedResolvedType.getType();
                        payloadType = payloadResolvedType.getType();

                        nestCount++;
                    }

                    // The types differs, pick expected name and payload type
                    if (expectedType != payloadType)
                    {
                        columns.add(new Column(expectedColumn.getName(), payloadColumn.getType()));
                    }
                    // Recurse into sub schema
                    else if (expectedType == Column.Type.Table
                            || expectedType == Column.Type.Object)
                    {
                        Schema subSchema = recreateSchema(expectedResolvedType.getSchema(), payloadResolvedType.getSchema(), expand);

                        ResolvedType arrayType = expectedType == Column.Type.Table ? ResolvedType.table(subSchema)
                                : ResolvedType.object(subSchema);

                        // Nest the new type
                        for (int j = 0; j < nestCount; j++)
                        {
                            arrayType = ResolvedType.array(arrayType);
                        }

                        columns.add(new Column(expectedColumn.getName(), arrayType));
                    }
                }
                // Equal => pick expected
                else
                {
                    columns.add(expectedColumn);
                }
            }
        }
        return new Schema(columns);
    }
}
