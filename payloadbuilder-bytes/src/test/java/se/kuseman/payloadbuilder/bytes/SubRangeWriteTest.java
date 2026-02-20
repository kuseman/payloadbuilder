package se.kuseman.payloadbuilder.bytes;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.nio.ByteBuffer;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * {@link NullBufferWriter} and {@link AReferenceVectorWriter} used to compute their internal offsets from the absolute row index instead of relative to the write range's start ({@code from}). Every
 * production call site happens to always pass {@code from == 0}, which masked the bug entirely - it only manifests once
 * {@link PayloadWriter#writeVector(BytesWriter, WriteCache, ValueVector, int, int)} is used with a non-zero {@code from}, which nothing in the codebase does today. These tests call that path directly
 * to prove the sub-range write is actually correct, not merely correct-by-coincidence-of-never-being-exercised.
 */
class SubRangeWriteTest
{
    @Test
    void test_scalar_subrange_preserves_null_positions()
    {
        // 6 rows total, row 3 (absolute) is null - write only the sub range [2, 5)
        ValueVector vector = vv(Type.Int, 100, 200, 300, null, 500, 600);

        BytesWriter writer = new BytesWriter();
        WriteCache cache = new WriteCache();
        PayloadWriter.writeVector(writer, cache, vector, 2, 5);

        ByteBuffer buffer = ByteBuffer.wrap(writer.toBytes())
                .order(PayloadReader.BYTE_ORDER);
        ValueVector actual = VectorFactory.getVector(buffer, 0, new ReadContext(), ResolvedType.of(Type.Int));

        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.Int, 300, null, 500), actual);
    }

    @Test
    void test_reference_type_subrange_preserves_values_and_null_positions()
    {
        // 6 rows total, row 3 (absolute) is null - write only the sub range [2, 5). This exercises
        // AReferenceVectorWriter's per-row header offsets, which IntVectorWriter (above) never touches.
        ValueVector vector = vv(Type.String, "a", "b", "c", null, "e", "f");

        BytesWriter writer = new BytesWriter();
        WriteCache cache = new WriteCache();
        PayloadWriter.writeVector(writer, cache, vector, 2, 5);

        ByteBuffer buffer = ByteBuffer.wrap(writer.toBytes())
                .order(PayloadReader.BYTE_ORDER);
        ValueVector actual = VectorFactory.getVector(buffer, 0, new ReadContext(), ResolvedType.of(Type.String));

        assertEquals(3, actual.size());
        assertVectorsEquals(vv(Type.String, "c", null, "e"), actual);
    }
}
