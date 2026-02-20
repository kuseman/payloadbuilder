package se.kuseman.payloadbuilder.bytes;

import static java.util.Collections.singletonMap;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.MetaData;
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
                    // validateResolvedType/expandType above already walked the full type tree, so `position`
                    // here is exactly "end of type tree" - use that range to fingerprint the payload's actual
                    // shape and check the cache before paying for a full re-parse + reconciliation rebuild.
                    // typeTreeHash is kept a primitive throughout (guarded by cacheEnabled, not nullability) so
                    // the common cache-enabled case never boxes it into a Long on every mismatched read.
                    SchemaReconciliationCache reconciliationCache = context.getReconciliationCache();
                    boolean cacheEnabled = reconciliationCache != null;
                    long typeTreeHash = 0L;
                    ResolvedType cachedPayloadType = null;
                    if (cacheEnabled)
                    {
                        typeTreeHash = Utils.hashRange(buffer, originalPosition, position);
                        cachedPayloadType = reconciliationCache.get(schema, typeTreeHash, context.isExpandSchema());
                    }

                    if (cachedPayloadType != null)
                    {
                        payloadType = cachedPayloadType;
                    }
                    else
                    {
                        buffer.position(originalPosition);
                        Reference<ResolvedType> ref = new Reference<>();
                        position = Utils.readResolvedType(buffer, originalPosition, ref);
                        payloadType = ResolvedType.table(recreateSchema(schema, ref.getValue()
                                .getSchema(), context.isExpandSchema()));

                        if (cacheEnabled)
                        {
                            reconciliationCache.put(schema, typeTreeHash, context.isExpandSchema(), payloadType);
                        }
                    }
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

    /**
     * Reconcile provided schemas into the schema that should actually be used when reading column data from the payload.
     *
     * <p>
     * If both schemas consistently carry column ids ({@link Utils#allColumnsHaveIds(Schema)}) columns are matched by id instead of position - this is what makes it safe to insert/remove/reorder
     * columns in the writer's schema over time without breaking old readers. Every resulting column (in id mode) is stamped with an internal {@link Utils#PHYSICAL_SLOT_KEY} metadata entry recording
     * which physical slot in {@code payload} it was found at (or {@code -1} if it wasn't found at all, which {@link BytesTupleVector#getColumn(int)} treats as a null column). This metadata is never
     * visible to callers - it only ever ends up on the internal "payload schema" used for data resolution, never on the schema returned by
     * {@link se.kuseman.payloadbuilder.api.execution.TupleVector#getSchema()}.
     * </p>
     *
     * <p>
     * If either schema is missing ids anywhere, falls back to the legacy position-based reconciliation (columns matched purely by index) - unchanged from before column ids existed.
     * </p>
     */
    private static Schema recreateSchema(Schema expected, Schema payload, boolean expand)
    {
        boolean idMode = Utils.allColumnsHaveIds(expected)
                && Utils.allColumnsHaveIds(payload);

        if (!idMode)
        {
            return recreateSchemaPositional(expected, payload, expand);
        }

        return recreateSchemaById(expected, payload, expand);
    }

    /** Id based reconciliation. See {@link #recreateSchema(Schema, Schema, boolean)}. */
    private static Schema recreateSchemaById(Schema expected, Schema payload, boolean expand)
    {
        int expectedSize = expected.getSize();
        int payloadSize = payload.getSize();

        Map<Integer, Integer> payloadIndexById = new HashMap<>();
        for (int i = 0; i < payloadSize; i++)
        {
            payloadIndexById.put(payload.getColumns()
                    .get(i)
                    .getMetaData()
                    .getColumnId(), i);
        }

        Set<Integer> matchedPayloadIndices = expand ? new HashSet<>()
                : null;

        List<Column> columns = new ArrayList<>(expectedSize);
        for (int i = 0; i < expectedSize; i++)
        {
            Column expectedColumn = expected.getColumns()
                    .get(i);
            Integer payloadIndex = payloadIndexById.get(expectedColumn.getMetaData()
                    .getColumnId());

            // Not present in this payload at all - either a column added to expected after this payload was
            // written, or removed from the writer's schema. Null-fill, keep expected name/type.
            if (payloadIndex == null)
            {
                columns.add(withPhysicalSlot(expectedColumn, -1));
                continue;
            }

            if (expand)
            {
                matchedPayloadIndices.add(payloadIndex);
            }

            Column payloadColumn = payload.getColumns()
                    .get(payloadIndex);
            Column reconciled = reconcileColumn(expectedColumn, payloadColumn, expand);
            columns.add(withPhysicalSlot(reconciled, payloadIndex));
        }

        if (expand)
        {
            for (int i = 0; i < payloadSize; i++)
            {
                if (!matchedPayloadIndices.contains(i))
                {
                    columns.add(withPhysicalSlot(payload.getColumns()
                            .get(i), i));
                }
            }
        }

        return new Schema(columns);
    }

    /** Reconcile a single matched (by id or by position) pair of columns. Extracted so both reconciliation strategies share the exact same type-drift/recursion rules. */
    private static Column reconcileColumn(Column expectedColumn, Column payloadColumn, boolean expand)
    {
        ResolvedType expectedResolvedType = expectedColumn.getType();
        ResolvedType payloadResolvedType = payloadColumn.getType();

        Column.Type expectedType = expectedResolvedType.getType();
        // Mismatch types => pick name from expected and type from payload
        if (expectedType != payloadResolvedType.getType())
        {
            return new Column(expectedColumn.getName(), payloadColumn.getType());
        }
        // Recurse into sub schema
        else if (expectedType == Column.Type.Table
                || expectedType == Column.Type.Object)
        {
            Schema subSchema = recreateSchema(expectedResolvedType.getSchema(), payloadResolvedType.getSchema(), expand);
            return new Column(expectedColumn.getName(), expectedType == Column.Type.Table ? ResolvedType.table(subSchema)
                    : ResolvedType.object(subSchema));
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
                return new Column(expectedColumn.getName(), payloadColumn.getType());
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

                return new Column(expectedColumn.getName(), arrayType);
            }
            // Equal => pick expected
            else
            {
                return new Column(expectedColumn.getName(), expectedColumn.getType());
            }
        }
        // Equal => pick expected
        return expectedColumn;
    }

    /** Return a copy of provided column stamped with the internal physical-slot metadata. Any other metadata on the column is dropped - this schema is internal only, never returned to callers. */
    private static Column withPhysicalSlot(Column column, int physicalSlot)
    {
        return new Column(column.getName(), column.getType(), new MetaData(singletonMap(Utils.PHYSICAL_SLOT_KEY, physicalSlot)));
    }

    /** Legacy, position based reconciliation used whenever either schema doesn't consistently carry column ids. Unchanged behavior from before column ids existed. */
    private static Schema recreateSchemaPositional(Schema expected, Schema payload, boolean expand)
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
                columns.add(reconcileColumn(expectedColumn, payloadColumn, expand));
            }
        }
        return new Schema(columns);
    }
}
