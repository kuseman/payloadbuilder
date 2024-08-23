package se.kuseman.payloadbuilder.bytes;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.List;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link PayloadWriter} */
public class PayloadWriterTest extends Assert
{
    @Test(
            expected = IllegalArgumentException.class)
    public void test_fail_any()
    {
        ValueVector v = vv(Type.Any, 1, 2, 3, 4);
        PayloadWriter.write(v);
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_invalid_payload()
    {
        PayloadReader.read(new byte[] { 1, 2, 3 });
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_invalid_payload_1()
    {
        PayloadReader.read(new byte[] { PayloadReader.P, PayloadReader.L, PayloadReader.B });
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_invalid_payload_2()
    {
        PayloadReader.read(new byte[] { PayloadReader.P, PayloadReader.L, PayloadReader.B, 1, 2, 3 });
    }

    @Test(
            expected = IllegalArgumentException.class)
    public void test_invalid_payload_3()
    {
        PayloadReader.read(new byte[] { PayloadReader.P, PayloadReader.L, 1, 2, PayloadReader.B });
    }

    @Test
    public void test_schema_recreation_with_more_expected_columns()
    {
        // @formatter:off
        Schema schemaV1 = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float)
                );

        TupleVector vectorV1 = TupleVector.of(schemaV1,
                asList(vv(Type.Int, 2, 1, 4, 3, 10),
                        vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F)));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(vectorV1, 1));

        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, false));
        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, true));

        // @formatter:off
        Schema schemaV2 = Schema.of(
                Column.of("float12", Column.Type.Float),
                Column.of("float", Column.Type.Float),
                Column.of("boolean", Column.Type.Boolean)
                );
        // @formatter:on

        TupleVector actual = PayloadReader.readTupleVector(bytes, schemaV2, false);

        assertEquals(schemaV2, actual.getSchema());

        VectorTestUtils.assertVectorsEquals(vv(Type.Int, 2, 1, 4, 3, 10), actual.getColumn(0));
        VectorTestUtils.assertVectorsEquals(vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F), actual.getColumn(1));
        VectorTestUtils.assertVectorsEquals(vv(Type.Boolean, null, null, null, null, null), actual.getColumn(2));

        actual = PayloadReader.readTupleVector(bytes, schemaV2, true);

        // @formatter:off
        schemaV2 = Schema.of(
                Column.of("float12", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("boolean", Column.Type.Boolean)
                );
        // @formatter:on

        assertEquals(actual.getSchema(), schemaV2);

        VectorTestUtils.assertVectorsEquals(vv(Type.Int, 2, 1, 4, 3, 10), actual.getColumn(0));
        VectorTestUtils.assertVectorsEquals(vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F), actual.getColumn(1));
        VectorTestUtils.assertVectorsEquals(vv(Type.Boolean, null, null, null, null, null), actual.getColumn(2));
    }

    @Test
    public void test_schema_diff_with_nested_array()
    {
        // @formatter:off
        Schema schemaV1 = Schema.of(
                Column.of("array", ResolvedType.array(ResolvedType.array(Column.Type.Int))));
        
        TupleVector vectorV1 = TupleVector.of(schemaV1, List.of(
                vv(ResolvedType.array(ResolvedType.array(Column.Type.Int)), vv(ResolvedType.array(Column.Type.Int), vv(Type.Int, 1,2,3)))
                ));
        // @formatter:off
        
        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(vectorV1, 1));

        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, false));
        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, true));

        // @formatter:off
        Schema schemaV2 = Schema.of(
                Column.of("arrayFloat", ResolvedType.array(ResolvedType.array(Column.Type.Float))));

        TupleVector vectorV2 = TupleVector.of(schemaV2, List.of(
                vv(ResolvedType.array(ResolvedType.array(Column.Type.Float)), vv(ResolvedType.array(Column.Type.Float), vv(Type.Float, 10.0F,20.F,30.F)))
                ));

        bytes = PayloadWriter.write(ValueVector.literalTable(vectorV2, 1));

        TupleVector actual = PayloadReader.readTupleVector(bytes, schemaV1, false);

        // Same original schema
        assertEquals(schemaV1, actual.getSchema());

        // Vectors should be payloads types
        VectorTestUtils.assertVectorsEquals(vv(ResolvedType.array(ResolvedType.array(Column.Type.Float)),
                        vv(ResolvedType.array(Column.Type.Float),
                                vv(Type.Float, 10.0F,20.F,30.F))), actual.getColumn(0));
        // @formatter:on
    }

    @Test
    public void test_schema_diff_with_nested_array_object()
    {
        // @formatter:off
        Schema innerSchema = Schema.of(Column.of("col", Column.Type.Int));

        Schema schemaV1 = Schema.of(
                Column.of("array", ResolvedType.array(ResolvedType.object(innerSchema))));

        TupleVector vectorV1 = TupleVector.of(schemaV1, List.of(
                vv(ResolvedType.array(ResolvedType.object(innerSchema)), vv(ResolvedType.object(innerSchema), ObjectVector.wrap(
                        TupleVector.of(innerSchema, List.of(
                                vv(Column.Type.Int, 6, null, 8)
                                ))
                        )))
                ));
        // @formatter:off

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(vectorV1, 1));

        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, false));
        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, true));

        // @formatter:off
        Schema innerSchemaV2 = Schema.of(Column.of("colNew", Column.Type.Boolean));

        Schema schemaV2 = Schema.of(
                Column.of("arrayNew", ResolvedType.array(ResolvedType.object(innerSchemaV2))));

        TupleVector vectorV2 = TupleVector.of(schemaV2, List.of(
                vv(ResolvedType.array(ResolvedType.object(innerSchemaV2)), vv(ResolvedType.object(innerSchemaV2), ObjectVector.wrap(
                        TupleVector.of(innerSchemaV2, List.of(
                                vv(Column.Type.Boolean, false, null, true)
                                ))
                        )))
                ));
        // @formatter:off

        bytes = PayloadWriter.write(ValueVector.literalTable(vectorV2, 1));

        // @formatter:off
        // Same name new type
        Schema expectedInnerSchema = Schema.of(Column.of("col", Column.Type.Boolean));

        Schema expectedSchema = Schema.of(
                // Same name new type
                Column.of("array", ResolvedType.array(ResolvedType.object(expectedInnerSchema))));

        TupleVector expectedVector = TupleVector.of(expectedSchema, List.of(
                vv(ResolvedType.array(ResolvedType.object(expectedInnerSchema)), vv(ResolvedType.object(expectedInnerSchema), ObjectVector.wrap(
                        TupleVector.of(expectedInnerSchema, List.of(
                                vv(Column.Type.Boolean, false, null, true)
                                ))
                        )))
                ));

        TupleVector actual = PayloadReader.readTupleVector(bytes, schemaV1, false);

        assertEquals(schemaV1, actual.getSchema());

        VectorTestUtils.assertVectorsEquals(expectedVector.getColumn(0), actual.getColumn(0));
        // @formatter:on
    }

    @Test
    public void test_schema_diff_with_nested_array_table()
    {
        // @formatter:off
        Schema innerSchema = Schema.of(Column.of("col", Column.Type.Int));

        Schema schemaV1 = Schema.of(
                Column.of("array", ResolvedType.array(ResolvedType.table(innerSchema))));

        TupleVector vectorV1 = TupleVector.of(schemaV1, List.of(
                vv(ResolvedType.array(ResolvedType.table(innerSchema)), vv(ResolvedType.table(innerSchema),
                        TupleVector.of(innerSchema, List.of(
                                vv(Column.Type.Int, 6, null, 8)
                                ))
                        ))
                ));
        // @formatter:off

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(vectorV1, 1));

        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, false));
        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, true));

        // @formatter:off
        Schema innerSchemaV2 = Schema.of(Column.of("colNew", Column.Type.Boolean));

        Schema schemaV2 = Schema.of(
                Column.of("arrayNew", ResolvedType.array(ResolvedType.table(innerSchemaV2))));

        TupleVector vectorV2 = TupleVector.of(schemaV2, List.of(
                vv(ResolvedType.array(ResolvedType.table(innerSchemaV2)), vv(ResolvedType.table(innerSchemaV2),
                        TupleVector.of(innerSchemaV2, List.of(
                                vv(Column.Type.Boolean, false, null, true)
                                ))
                        ))
                ));
        // @formatter:off

        bytes = PayloadWriter.write(ValueVector.literalTable(vectorV2, 1));

        // @formatter:off
        // Same name new type
        Schema expectedInnerSchema = Schema.of(Column.of("col", Column.Type.Boolean));

        Schema expectedSchema = Schema.of(
                // Same name new type
                Column.of("array", ResolvedType.array(ResolvedType.table(expectedInnerSchema))));

        TupleVector expectedVector = TupleVector.of(expectedSchema, List.of(
                vv(ResolvedType.array(ResolvedType.table(expectedInnerSchema)), vv(ResolvedType.table(expectedInnerSchema),
                        TupleVector.of(expectedInnerSchema, List.of(
                                vv(Column.Type.Boolean, false, null, true)
                                )
                        )))
                ));
        
        TupleVector actual = PayloadReader.readTupleVector(bytes, schemaV1, false);

        assertEquals(schemaV1, actual.getSchema());

        VectorTestUtils.assertVectorsEquals(expectedVector.getColumn(0), actual.getColumn(0));
        // @formatter:on
    }

    @Test
    public void test_schema_diff_with_nested_table()
    {
        // @formatter:off
        Schema innerSchemaV1 = Schema.of(
                Column.of("subBool", Column.Type.Boolean),
                Column.of("subDouble", Column.Type.Double)
                );
        
        Schema schemaV1 = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("table", ResolvedType.table(innerSchemaV1)
                ));
        
        TupleVector vectorV1 = TupleVector.of(schemaV1, List.of(
                vv(Type.Int, 1, 2, 3),
                vv(ResolvedType.table(innerSchemaV1),
                        TupleVector.of(innerSchemaV1, List.of(
                                vv(Type.Boolean, true, null),
                                vv(Type.Double, 1.0D, -10.0D)
                                )),
                        TupleVector.of(innerSchemaV1, List.of(
                                vv(Type.Boolean, false),
                                vv(Type.Double, 666.0D)
                                )),
                        TupleVector.of(innerSchemaV1, List.of(
                                vv(Type.Boolean, false, null),
                                vv(Type.Double, 7.0D, -13.0D)
                                ))
                )));
        //@formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(vectorV1, 1));

        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, false));

        // Now change a column inside sub table

        // @formatter:off
        Schema innerSchemaV2 = Schema.of(
                Column.of("subInteger", Column.Type.Int),
                Column.of("subDouble", Column.Type.Double)
                );

        Schema schemaV2 = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("table", ResolvedType.table(innerSchemaV2)
                ));

        TupleVector vectorV2 = TupleVector.of(schemaV2, List.of(
                vv(Type.Int, 1, 2, 3),
                vv(ResolvedType.table(innerSchemaV2),
                        TupleVector.of(innerSchemaV2, List.of(
                                vv(Type.Int, 1, null),
                                vv(Type.Double, 1.0D, -10.0D)
                                )),
                        TupleVector.of(innerSchemaV2, List.of(
                                vv(Type.Int, 2),
                                vv(Type.Double, 666.0D)
                                )),
                        TupleVector.of(innerSchemaV2, List.of(
                                vv(Type.Int, -30, 1337),
                                vv(Type.Double, 7.0D, -13.0D)
                                ))
                )));
        //@formatter:on

        bytes = PayloadWriter.write(ValueVector.literalTable(vectorV2, 1));

        // Verify current
        VectorTestUtils.assertTupleVectorsEquals(vectorV2, PayloadReader.readTupleVector(bytes, schemaV2, false));

        // Verify old schema against new vector
        TupleVector actual = PayloadReader.readTupleVector(bytes, schemaV1, false);
        // @formatter:off
        Schema expectedInnerSchema = Schema.of(
                // Old name but new type
                Column.of("subBool", Column.Type.Int),
                Column.of("subDouble", Column.Type.Double)
                );

        assertEquals(schemaV1, actual.getSchema());

        VectorTestUtils.assertVectorsEquals(vv(Type.Int, 1, 2, 3), actual.getColumn(0));
        VectorTestUtils.assertVectorsEquals(vv(ResolvedType.table(expectedInnerSchema),
                TupleVector.of(expectedInnerSchema, List.of(
                        vv(Type.Int, 1, null),
                        vv(Type.Double, 1.0D, -10.0D)
                        )),
                TupleVector.of(expectedInnerSchema, List.of(
                        vv(Type.Int, 2),
                        vv(Type.Double, 666.0D)
                        )),
                TupleVector.of(expectedInnerSchema, List.of(
                        vv(Type.Int, -30, 1337),
                        vv(Type.Double, 7.0D, -13.0D)
                        ))
        ), actual.getColumn(1));
        //@formatter:on
    }

    @Test
    public void test_schema_diff_with_nested_object()
    {
        // @formatter:off
        Schema innerSchemaV1 = Schema.of(
                Column.of("subBool", Column.Type.Boolean),
                Column.of("subDouble", Column.Type.Double)
                );
        
        Schema schemaV1 = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("table", ResolvedType.object(innerSchemaV1)
                ));

        TupleVector vectorV1 = TupleVector.of(schemaV1, List.of(
                vv(Type.Int, 1, 2, 3),
                vv(ResolvedType.object(innerSchemaV1),
                        ObjectVector.wrap(TupleVector.of(innerSchemaV1, List.of(
                                vv(Type.Boolean, true, null),
                                vv(Type.Double, 1.0D, -10.0D)
                                ))),
                        ObjectVector.wrap(TupleVector.of(innerSchemaV1, List.of(
                                vv(Type.Boolean, false),
                                vv(Type.Double, 666.0D)
                                ))),
                        ObjectVector.wrap(TupleVector.of(innerSchemaV1, List.of(
                                vv(Type.Boolean, false, null),
                                vv(Type.Double, 7.0D, -13.0D)
                                )))
                )));
        //@formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(vectorV1, 1));

        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, false));

        // Now change a column inside sub table

        // @formatter:off
        Schema innerSchemaV2 = Schema.of(
                Column.of("subInteger", Column.Type.Int),
                Column.of("subDouble", Column.Type.Double)
                );

        Schema schemaV2 = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("table", ResolvedType.object(innerSchemaV2)
                ));

        TupleVector vectorV2 = TupleVector.of(schemaV2, List.of(
                vv(Type.Int, 1, 2, 3),
                vv(ResolvedType.object(innerSchemaV2),
                        ObjectVector.wrap(TupleVector.of(innerSchemaV2, List.of(
                                vv(Type.Int, 1, null),
                                vv(Type.Double, 1.0D, -10.0D)
                                ))),
                        ObjectVector.wrap(TupleVector.of(innerSchemaV2, List.of(
                                vv(Type.Int, 2),
                                vv(Type.Double, 666.0D)
                                ))),
                        ObjectVector.wrap(TupleVector.of(innerSchemaV2, List.of(
                                vv(Type.Int, -30, 1337),
                                vv(Type.Double, 7.0D, -13.0D)
                                )))
                )));
        //@formatter:on

        bytes = PayloadWriter.write(ValueVector.literalTable(vectorV2, 1));

        // Verify current
        VectorTestUtils.assertTupleVectorsEquals(vectorV2, PayloadReader.readTupleVector(bytes, schemaV2, false));

        // Verify old schema against new vector
        TupleVector actual = PayloadReader.readTupleVector(bytes, schemaV1, false);
        // @formatter:off
        Schema expectedInnerSchema = Schema.of(
                // Old name but new type
                Column.of("subBool", Column.Type.Int),
                Column.of("subDouble", Column.Type.Double)
                );
        // Verify that the schema still is the same
        assertEquals(schemaV1, actual.getSchema());

        // But the vectors are of the payloads type
        VectorTestUtils.assertVectorsEquals(vv(Type.Int, 1, 2, 3), actual.getColumn(0));
        VectorTestUtils.assertVectorsEquals(vv(ResolvedType.object(expectedInnerSchema),
                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, List.of(
                        vv(Type.Int, 1, null),
                        vv(Type.Double, 1.0D, -10.0D)
                        ))),
                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, List.of(
                        vv(Type.Int, 2),
                        vv(Type.Double, 666.0D)
                        ))),
                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, List.of(
                        vv(Type.Int, -30, 1337),
                        vv(Type.Double, 7.0D, -13.0D)
                        )))
        ), actual.getColumn(1));
    }

    /** Test a migration flow where we obsolete old columns and the switches the data type */
    @Test
    public void test_schema_migration_flow()
    {
        // @formatter:off
        Schema schemaV1 = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("boolean", Column.Type.Boolean),
                Column.of("array", ResolvedType.array(Column.Type.Float)),
                Column.of("string1", Column.Type.String)
                );

        TupleVector vectorV1 = TupleVector.of(schemaV1,
                asList(vv(Type.Int, 2, 1, 4, 3, 10),
                        vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        vv(Type.Boolean, true, false, null, false, true),
                        vv(ResolvedType.array(Column.Type.Float),
                                vv(Type.Float, 1.0f, 2.0f),
                                vv(Type.Float, 4.0f, 5.0f, 666.0f),
                                vv(Type.Float, 6.0f, 7.0f),
                                vv(Type.Float, 8.0f),
                                vv(Type.Float, 10.0f, null)
                                ),
                        vv(Type.String, "one", "two", "three", null, "five")));
        // @formatter:on

        byte[] bytes = PayloadWriter.write(ValueVector.literalTable(vectorV1, 1));

        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, false));
        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, true));

        // Now we want to obsolete the float array column and have a integer array instead
        // First write new column along with old data

        //@formatter:off
        Schema schemaV2 = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("boolean", Column.Type.Boolean),
                Column.of("array", ResolvedType.array(Column.Type.Float)),
                Column.of("string1", Column.Type.String),
                Column.of("arrayInt", ResolvedType.array(Column.Type.Int))
                );

        TupleVector vectorV2 = TupleVector.of(schemaV2,
                asList(vv(Type.Int, 2, 1, 4, 3, 10),
                        vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        vv(Type.Boolean, true, false, null, false, true),
                        vv(ResolvedType.array(Column.Type.Float),
                                vv(Type.Float, 1.0f, 2.0f),
                                vv(Type.Float, 4.0f, 5.0f, 666.0f),
                                vv(Type.Float, 6.0f, 7.0f),
                                vv(Type.Float, 8.0f),
                                vv(Type.Float, 10.0f, null)
                                ),
                        vv(Type.String, "one", "two", "three", null, "five"),
                        vv(ResolvedType.array(Column.Type.Int),
                                vv(Type.Int, 1, 2),
                                vv(Type.Int, 4, 5, 6),
                                vv(Type.Int, 6, 7),
                                vv(Type.Int, 8),
                                vv(Type.Int, 10, null)
                                )
                        ));
        //@formatter:off

        // Write new payload with extra column
        bytes = PayloadWriter.write(ValueVector.literalTable(vectorV2, 1));

        // Verify that old version still works with new payload
        VectorTestUtils.assertTupleVectorsEquals(vectorV1, PayloadReader.readTupleVector(bytes, schemaV1, false));

        // Write new payload with new datatype on obsolete column
        //@formatter:off
        schemaV2 = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("boolean", Column.Type.Boolean),
                Column.of("string", Column.Type.String),
                Column.of("string1", Column.Type.String),
                Column.of("arrayInt", ResolvedType.array(Column.Type.Int))
                );

        vectorV2 = TupleVector.of(schemaV2,
                asList(vv(Type.Int, 2, 1, 4, 3, 10),
                        vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        vv(Type.Boolean, true, false, null, false, true),
                        // Now we don't have an array of floats any more on this slot
                        vv(Type.String, "hello", "world", null, "one", "two"),
                        vv(Type.String, "one", "two", "three", null, "five"),
                        vv(ResolvedType.array(Column.Type.Int),
                                vv(Type.Int, 1, 2),
                                vv(Type.Int, 4, 5, 6),
                                vv(Type.Int, 6, 7),
                                vv(Type.Int, 8),
                                vv(Type.Int, 10, null)
                                )
                        ));
        bytes = PayloadWriter.write(ValueVector.literalTable(vectorV2, 1));

        // Verify that old version still works
        TupleVector oldSchemaNewPayload = PayloadReader.readTupleVector(bytes, schemaV1, false);

        // We should still have the input schema on the payload, this to be able to
        // have correct schema according to a compiled query in PLB etc.
        assertEquals(schemaV1, oldSchemaNewPayload.getSchema());

        VectorTestUtils.assertVectorsEquals(vv(Type.Int, 2, 1, 4, 3, 10), oldSchemaNewPayload.getColumn(0));
        VectorTestUtils.assertVectorsEquals(vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F), oldSchemaNewPayload.getColumn(1));
        VectorTestUtils.assertVectorsEquals(vv(Type.Boolean, true, false, null, false, true), oldSchemaNewPayload.getColumn(2));

        // Now column 3 is a String vector even if the expected schema says array<int>
        VectorTestUtils.assertVectorsEquals(vv(Type.String, "hello", "world", null, "one", "two"), oldSchemaNewPayload.getColumn(3));

        // If we try to access the old schema type we will end up with an exception
        // Hence it's important be make sure that these columns are not used in a non implicit cast way
        try
        {
            oldSchemaNewPayload.getColumn(3).getArray(0);
            fail("Should fail");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage().contains("Cannot cast String to Array"));
        }

        VectorTestUtils.assertVectorsEquals(vv(Type.String, "one", "two", "three", null, "five"), oldSchemaNewPayload.getColumn(4));

        // Verify expansion as well
        oldSchemaNewPayload = PayloadReader.readTupleVector(bytes, schemaV1, true);

        assertEquals(Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("boolean", Column.Type.Boolean),
                // This column is still called array in old schema but now the type is the payloads => String
                Column.of("array", Column.Type.String),
                Column.of("string1", Column.Type.String),
                Column.of("array_5", ResolvedType.array(Column.Type.Int))
                ), oldSchemaNewPayload.getSchema());

        VectorTestUtils.assertVectorsEquals(vv(Type.Int, 2, 1, 4, 3, 10), oldSchemaNewPayload.getColumn(0));
        VectorTestUtils.assertVectorsEquals(vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F), oldSchemaNewPayload.getColumn(1));
        VectorTestUtils.assertVectorsEquals(vv(Type.Boolean, true, false, null, false, true), oldSchemaNewPayload.getColumn(2));
        // Now column 3 is a String vector even if the expected schema says array<int>
        VectorTestUtils.assertVectorsEquals(vv(Type.String, "hello", "world", null, "one", "two"), oldSchemaNewPayload.getColumn(3));
        VectorTestUtils.assertVectorsEquals(vv(Type.String, "one", "two", "three", null, "five"), oldSchemaNewPayload.getColumn(4));
        VectorTestUtils.assertVectorsEquals(vv(ResolvedType.array(Column.Type.Int), vv(Type.Int, 1, 2),
                vv(Type.Int, 4, 5, 6),
                vv(Type.Int, 6, 7),
                vv(Type.Int, 8),
                vv(Type.Int, 10, null)), oldSchemaNewPayload.getColumn(5));
        //@formatter:on

        // Verify new schema works without any ignores
        VectorTestUtils.assertTupleVectorsEquals(vectorV2, PayloadReader.readTupleVector(bytes, schemaV2, false));
        VectorTestUtils.assertTupleVectorsEquals(vectorV2, PayloadReader.readTupleVector(bytes, schemaV2, true));

        // Now we can undeploy old version and start using the new version fully
        // and have now migrated data types of an existing column without any downtime
    }

    @Test
    public void test_array()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(ResolvedType.array(Type.Int));

        bytes = PayloadWriter.write(v);

        assertEquals(9, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = vv(ResolvedType.array(Type.Decimal), vv(Type.Decimal, 1692013080000L), vv(Type.Decimal, 1691013080000L), null, vv(Type.Decimal, 1690013080000L), vv(Type.Decimal, 1612013080000L),
                vv(Type.Decimal, 1672013080000L), vv(Type.Decimal, 1692013080000L), vv(Type.Decimal, 1622013080000L), vv(Type.Decimal, 1652013080000L));

        bytes = PayloadWriter.write(v);

        assertEquals(183, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // No nulls
        v = VectorTestUtils.vv(ResolvedType.array(Type.Int), vv(Type.Int, 1), vv(Type.Int, 2), vv(Type.Int, 3), vv(Type.Int, 4), vv(Type.Int, 5), vv(Type.Int, 6), vv(Type.Int, 7), vv(Type.Int, 8));

        bytes = PayloadWriter.write(v);

        assertEquals(107, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Decimal, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Test array of arrays
        //@formatter:off
        v = VectorTestUtils.vv(ResolvedType.array(ResolvedType.array(Type.Int)),
                vv(ResolvedType.array(Type.Int), vv(Type.Int, 1), vv(Type.Int, 2)),
                vv(ResolvedType.array(Type.Int), vv(Type.Int, 3), vv(Type.Int, 4)),
                vv(ResolvedType.array(Type.Int), vv(Type.Int, 5), vv(Type.Int, 6)),
                vv(ResolvedType.array(Type.Int), vv(Type.Int, 7), vv(Type.Int, 8)));
        //@formatter:on

        bytes = PayloadWriter.write(v);

        assertEquals(140, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_decimal()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Decimal);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Decimal, 1692013080000L, 1691013080000L, null, 1690013080000L, 1612013080000L, 1672013080000L, 1692013080000L, 1622013080000L, 1652013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(118, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // No nulls
        v = VectorTestUtils.vv(Type.Decimal, 1692013080000L, 1691013080000L, 1690013080000L, 1612013080000L, 1672013080000L, 1692013080000L, 1622013080000L, 1652013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(112, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.Decimal, 1692013080000L, 1692013080000L, 1692013080000L, 1692013080000L, 1692013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(24, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Decimal, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_datetime()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.DateTime);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.DateTime, 1692013080000L, 1691013080000L, null, 1690013080000L, 1612013080000L, 1672013080000L, 1692013080000L, 1622013080000L, 1652013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(104, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // No nulls
        v = VectorTestUtils.vv(Type.DateTime, 1692013080000L, 1690013080000L, 1692013080000L, 1690013080000L, 1692013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(46, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.DateTime, 1692013080000L, 1692013080000L, 1692013080000L, 1692013080000L, 1692013080000L);

        bytes = PayloadWriter.write(v);

        assertEquals(22, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.DateTime, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_int()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Int);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Int, 1, 2, null, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(48, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        assertEquals(14, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Int, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_long_cache()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Test without cache
        v = VectorTestUtils.vv(Type.Long, 1, 2, 3, 4, 5);

        bytes = PayloadWriter.write(v);

        // meta 5
        // type 1
        // length 1
        // nullCount 1
        // version 1
        // encoding 1
        // headers 5 * 4 = 20
        // 5 longs = 5 * 8 = 40
        // total: 70
        assertEquals(70, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Test cache
        v = VectorTestUtils.vv(Type.Long, 1, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        // meta 5
        // type 1
        // length 1
        // nullCount 1
        // version 1
        // encoding 1
        // headers 5 * 4 = 20
        // 2 longs = 2 * 8 = 16
        // total: 46
        assertEquals(46, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Test literal cache

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("long", Column.Type.Long),
                Column.of("long1", Column.Type.Long));
        
        TupleVector tv = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Long, 1, 2),
                        VectorTestUtils.vv(Type.Long, 2, 2)));      // Long value 2 is cached in previous vector
        //@formatter:on

        bytes = PayloadWriter.write(ValueVector.literalTable(tv, 1));

        assertEquals(63, bytes.length);

        actual = PayloadReader.read(bytes);

        // @formatter:off
        Schema expectedSchema = Schema.of(
                Column.of("long_0", Column.Type.Long),
                Column.of("long_1", Column.Type.Long));
        
        TupleVector expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Long, 1, 2),
                        VectorTestUtils.vv(Type.Long, 2, 2)));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual.getTable(0));
    }

    @Test
    public void test_double_cache()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Test without cache
        v = VectorTestUtils.vv(Type.Double, 1, 2, 3, 4, 5);

        bytes = PayloadWriter.write(v);

        // meta 5
        // type 1
        // length 1
        // nullCount 1
        // version 1
        // encoding 1
        // headers 5 * 4 = 20
        // 5 doubles = 5 * 8 = 40
        // total: 70
        assertEquals(70, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Test cache
        v = VectorTestUtils.vv(Type.Double, 1, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        // meta 5
        // type 1
        // length 1
        // nullCount 1
        // version 1
        // encoding 1
        // headers 5 * 4 = 20
        // 2 doubles = 2 * 8 = 16
        // total: 46
        assertEquals(46, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Test literal cache

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("long", Column.Type.Double),
                Column.of("long1", Column.Type.Double));
        
        TupleVector tv = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Double, 1, 2),
                        VectorTestUtils.vv(Type.Double, 2, 2)));      // Double value 2 is cached in previous vector
        //@formatter:on

        bytes = PayloadWriter.write(ValueVector.literalTable(tv, 1));

        assertEquals(63, bytes.length);

        actual = PayloadReader.read(bytes);

        // @formatter:off
        Schema expectedSchema = Schema.of(
                Column.of("double_0", Column.Type.Double),
                Column.of("double_1", Column.Type.Double));
        
        TupleVector expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Double, 1, 2),
                        VectorTestUtils.vv(Type.Double, 2, 2)));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual.getTable(0));
    }

    @Test
    public void test_long()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Long);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Long, 1, 2, null, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(112, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Long, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(118, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.Long, 2, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        assertEquals(22, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Long, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_string()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Long);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.String, 1, "hello", null, 4, "hello", 6, 7, 8, "world");

        bytes = PayloadWriter.write(v);

        assertEquals(70, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.String, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(64, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.String, "sv", "sv", "sv", "sv");

        bytes = PayloadWriter.write(v);

        assertEquals(17, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.String, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_double()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Double);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Double, 1, 2, null, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(112, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Double, 1, 2, 3, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(118, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.Double, 2, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        assertEquals(22, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Double, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_boolean()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Boolean);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Boolean, true, false, null, true, true, true, null, false, false);

        bytes = PayloadWriter.write(v);

        assertEquals(15, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Boolean, true, false, true, true, true, false, false);

        bytes = PayloadWriter.write(v);

        assertEquals(12, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal true
        v = VectorTestUtils.vv(Type.Boolean, true, true, true, true);

        bytes = PayloadWriter.write(v);

        assertEquals(11, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal false
        v = VectorTestUtils.vv(Type.Boolean, false, false, false, false);

        bytes = PayloadWriter.write(v);

        assertEquals(11, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Boolean, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    @Test
    public void test_float()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;

        // Empty
        v = VectorTestUtils.vv(Type.Float);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        v = VectorTestUtils.vv(Type.Float, 1, 2, null, 4, 5, 6, 7, 8, 9);

        bytes = PayloadWriter.write(v);

        assertEquals(48, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal
        v = VectorTestUtils.vv(Type.Float, 2, 2, 2, 2, 2);

        bytes = PayloadWriter.write(v);

        assertEquals(14, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);

        // Literal null
        v = VectorTestUtils.vv(Type.Float, null, null, null, null, null);

        bytes = PayloadWriter.write(v);

        assertEquals(8, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(v, actual);
    }

    /**
     * Regression where we had a mismatch in input schema and payload schema
     * where equal arrays wasn't re-added correctly.
     */
    @Test
    public void test_table_with_array_1()
    {
        ValueVector v;
        byte[] bytes;
        TupleVector expected;

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("array", ResolvedType.array(Type.Int)));

        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        vv(ResolvedType.array(Type.Int), vv(Type.Int, 1,2), vv(Type.Int, 3,4), vv(Type.Int, 5,6), vv(Type.Int, 7,8), vv(Type.Int, 9,10))
                        ));
        //@formatter:on

        v = ValueVector.literalTable(expected, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(120, bytes.length);

        // Read with same schema

        TupleVector actual = PayloadReader.readTupleVector(bytes, schema, false);

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // @formatter:off
        Schema newSchema = Schema.of(
                Column.of("integer", Column.Type.Boolean),          // Trigg a mismatch
                Column.of("array", ResolvedType.array(Type.Int)));

        // Read with a new schema that mismatches
        actual = PayloadReader.readTupleVector(bytes, newSchema, false);

        // Schema should reflect the new input schema
        assertEquals(newSchema, actual.getSchema());

        // .. but the vectors are of the actual payload types

        VectorTestUtils.assertVectorsEquals(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2), actual.getColumn(0));
        VectorTestUtils.assertVectorsEquals(vv(ResolvedType.array(Type.Int), vv(Type.Int, 1,2), vv(Type.Int, 3,4), vv(Type.Int, 5,6), vv(Type.Int, 7,8), vv(Type.Int, 9,10)), actual.getColumn(1));
        // @formatter:on
    }

    @Test
    public void test_table_with_array()
    {
        ValueVector v;
        byte[] bytes;
        TupleVector expected;
        Schema expectedSchema;

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("array4", ResolvedType.array(Type.Int)));

        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5),
                        vv(ResolvedType.array(Type.Int), vv(Type.Int, 1,2), vv(Type.Int, 3,4), vv(Type.Int, 5,6), vv(Type.Int, 7,8), vv(Type.Int, 9,10))
                        ));
        //@formatter:on

        v = ValueVector.literalTable(expected, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(170, bytes.length);

        // Read with same schema

        TupleVector actual = PayloadReader.readTupleVector(bytes, schema, false);

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Read with no schema

        actual = PayloadReader.readTupleVector(bytes, Schema.EMPTY, false);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float),
                Column.of("array_4", ResolvedType.array(Type.Int)));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5),
                        vv(ResolvedType.array(Type.Int), vv(Type.Int, 1,2), vv(Type.Int, 3,4), vv(Type.Int, 5,6), vv(Type.Int, 7,8), vv(Type.Int, 9,10))
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Read with less columns schema no expand

        // @formatter:off
        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float));
        //@formatter:on

        actual = PayloadReader.readTupleVector(bytes, schema, false);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F)
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Read with less columns schema with expand

        // @formatter:off
        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float));
        //@formatter:on

        actual = PayloadReader.readTupleVector(bytes, schema, true);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float),
                Column.of("array_4", ResolvedType.array(Type.Int)));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5),
                        vv(ResolvedType.array(Type.Int), vv(Type.Int, 1,2), vv(Type.Int, 3,4), vv(Type.Int, 5,6), vv(Type.Int, 7,8), vv(Type.Int, 9,10))
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Read with same schema with expand

        actual = PayloadReader.readTupleVector(bytes, expectedSchema, true);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float),
                Column.of("array_4", ResolvedType.array(Type.Int)));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5),
                        vv(ResolvedType.array(Type.Int), vv(Type.Int, 1,2), vv(Type.Int, 3,4), vv(Type.Int, 5,6), vv(Type.Int, 7,8), vv(Type.Int, 9,10))
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);
    }

    @Test
    public void test_table_context_schema()
    {
        ValueVector v;
        TupleVector actual;
        TupleVector expected;
        byte[] bytes;
        Schema expectedSchema;
        Schema expectedInnerSchema;

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float));

        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5)));
        //@formatter:on

        v = ValueVector.literalTable(expected, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(80, bytes.length);

        actual = PayloadReader.readTupleVector(bytes, schema, false);

        // Verify that the schema was not re-created
        assertSame(schema, actual.getSchema());

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Test with less columns without expand

        // @formatter:off
        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float));
        // @formatter:off

        actual = PayloadReader.readTupleVector(bytes, schema, false);
        assertSame(schema, actual.getSchema());

        // @formatter:off
        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F)
                        // No last column here since the input schema doesn't have it and we don't expand
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Test with more columns, and null is returned

        // @formatter:off
        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("boolean5", Column.Type.Boolean));
        // @formatter:off

        actual = PayloadReader.readTupleVector(bytes, schema, false);

        // @formatter:off
        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5),
                        ValueVector.literalNull(ResolvedType.of(Type.Boolean), 5)
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Test with less columns with expand

        // @formatter:off
        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float));
        // @formatter:off

        actual = PayloadReader.readTupleVector(bytes, schema, true);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float));     // auto expanded column

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5)
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Test nested table
        // @formatter:off
        Schema innerSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float)
                );

        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table1", ResolvedType.table(innerSchema)),
                Column.of("object1", ResolvedType.object(innerSchema))
                );

        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(innerSchema),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(innerSchema),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        v = ValueVector.literalTable(expected, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(198, bytes.length);

        actual = PayloadReader.readTupleVector(bytes, schema, false);

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Test with no schema, all columns auto generated

        actual = PayloadReader.readTupleVector(bytes, Schema.EMPTY, true);

        //@formatter:off
        expectedInnerSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float)
                );

        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float),
                Column.of("table_4", ResolvedType.table(expectedInnerSchema)),
                Column.of("object_5", ResolvedType.object(expectedInnerSchema))
                );

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(expectedInnerSchema),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(expectedInnerSchema),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // One column short in nested object/table

        // @formatter:off
        innerSchema = Schema.of(
                Column.of("integer", Column.Type.Int)
                );

        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table1", ResolvedType.table(innerSchema)),
                Column.of("object1", ResolvedType.object(innerSchema))
                );

        actual = PayloadReader.readTupleVector(bytes, schema, true);

        //@formatter:off
        expectedInnerSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float_1", Column.Type.Float)
                );

        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table1", ResolvedType.table(expectedInnerSchema)),
                Column.of("object1", ResolvedType.object(expectedInnerSchema))
                );

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(expectedInnerSchema),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(expectedInnerSchema),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // More columns in inner schema than payload without expand

        // @formatter:off
        innerSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("decimal", Column.Type.Decimal)
                );

        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table_4", ResolvedType.table(innerSchema)),
                Column.of("object_5", ResolvedType.object(innerSchema))
                );

        actual = PayloadReader.readTupleVector(bytes, schema, false);

        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(innerSchema),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        )),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(innerSchema),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // More columns in inner schema than payload with expand

        // @formatter:off
        innerSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("decimal", Column.Type.Decimal)
                );

        schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table1", ResolvedType.table(innerSchema)),
                Column.of("object1", ResolvedType.object(innerSchema))
                );

        actual = PayloadReader.readTupleVector(bytes, schema, true);

        expected = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(innerSchema),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        )),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(innerSchema),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F),
                                        ValueVector.literalNull(ResolvedType.of(Type.Decimal), 2)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);

        // Missing whole table and object in provided schema

        schema = Schema.of(Column.of("integer", Column.Type.Int), Column.of("float", Column.Type.Float), Column.of("float2", Column.Type.Float), Column.of("float3", Column.Type.Float));

        actual = PayloadReader.readTupleVector(bytes, schema, true);

        //@formatter:off
        expectedInnerSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float)
                );

        expectedSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table_4", ResolvedType.table(expectedInnerSchema)),
                Column.of("object_5", ResolvedType.object(expectedInnerSchema))
                );

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(expectedInnerSchema),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(expectedInnerSchema),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        )), 1)
                                )
                        ));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual);
    }

    @Test
    public void test_table()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;
        Schema expectedSchema;
        TupleVector expected;

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float));

        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float));

        TupleVector tv = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5)));
        //@formatter:on

        // Empty
        v = VectorTestUtils.vv(ResolvedType.table(schema));

        bytes = PayloadWriter.write(v);

        assertEquals(13, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(VectorTestUtils.vv(ResolvedType.table(expectedSchema)), actual);

        v = ValueVector.literalTable(tv, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(80, bytes.length);

        actual = PayloadReader.read(bytes);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5)));
        //@formatter:on

        VectorTestUtils.assertTupleVectorsEquals(expected, actual.getTable(0));
    }

    @Test
    public void test_object()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;
        Schema expectedSchema;
        TupleVector expected;

        // @formatter:off
        Schema schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float));

        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float));

        TupleVector tv = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2, 2, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, 3.0F, null, 5.0F, 6.0F),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F, 33.0F, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 5)));

        ObjectVector ov = ObjectVector.wrap(tv, 2);
        //@formatter:on

        // Empty
        v = VectorTestUtils.vv(ResolvedType.object(schema));

        bytes = PayloadWriter.write(v);

        assertEquals(13, bytes.length);
        actual = PayloadReader.read(bytes);

        VectorTestUtils.assertVectorsEquals(VectorTestUtils.vv(ResolvedType.object(expectedSchema)), actual);

        v = ValueVector.literalObject(ov, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(53, bytes.length);

        actual = PayloadReader.read(bytes);

        // @formatter:off
        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2),
                        VectorTestUtils.vv(Type.Float, (Float) null),
                        VectorTestUtils.vv(Type.Float, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 1)));
        //@formatter:on

        VectorTestUtils.assertVectorsEquals(ValueVector.literalObject(ObjectVector.wrap(expected), 1), actual);
    }

    @Test
    public void test_nested_table()
    {
        ValueVector v;
        ValueVector actual;
        byte[] bytes;
        Schema expectedSchema;
        TupleVector expected;

        // @formatter:off
        Schema innerSchema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float)
                );

        Schema schema = Schema.of(
                Column.of("integer", Column.Type.Int),
                Column.of("float", Column.Type.Float),
                Column.of("float2", Column.Type.Float),
                Column.of("float3", Column.Type.Float),
                Column.of("table1", ResolvedType.table(innerSchema)),
                Column.of("object1", ResolvedType.object(innerSchema))
                );

        TupleVector tv = TupleVector.of(schema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(innerSchema), 
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(innerSchema),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(innerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 123, 666),
                                        VectorTestUtils.vv(Type.Float, null, 456, 1234F)
                                        )), 2)
                                )
                        ));
        //@formatter:on

        v = ValueVector.literalTable(tv, 1);
        bytes = PayloadWriter.write(v);

        assertEquals(198, bytes.length);
        actual = PayloadReader.read(bytes);

        // @formatter:off
        Schema expectedInnerSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float)
                );

        expectedSchema = Schema.of(
                Column.of("int_0", Column.Type.Int),
                Column.of("float_1", Column.Type.Float),
                Column.of("float_2", Column.Type.Float),
                Column.of("float_3", Column.Type.Float),
                Column.of("table_4", ResolvedType.table(expectedInnerSchema)),
                Column.of("object_5", ResolvedType.object(expectedInnerSchema)));

        expected = TupleVector.of(expectedSchema,
                asList(VectorTestUtils.vv(Type.Int, 2, 2),
                        VectorTestUtils.vv(Type.Float, 2.0F, null),
                        VectorTestUtils.vv(Type.Float, 33.0F, 33.0F),
                        ValueVector.literalNull(ResolvedType.of(Type.Float), 2),
                        VectorTestUtils.vv(ResolvedType.table(expectedInnerSchema),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12, 22),
                                        VectorTestUtils.vv(Type.Float, 22.0F, 666.50F)
                                        )),
                                TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666, 666),
                                        VectorTestUtils.vv(Type.Float, null, 1234F)
                                        ))
                                ),
                        VectorTestUtils.vv(ResolvedType.object(expectedInnerSchema),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 12),
                                        VectorTestUtils.vv(Type.Float, 22.0F)
                                        ))),
                                ObjectVector.wrap(TupleVector.of(expectedInnerSchema, asList(
                                        VectorTestUtils.vv(Type.Int, 666),
                                        VectorTestUtils.vv(Type.Float, 1234F)
                                        )))
                                )
                        ));
        //@formatter:on
        VectorTestUtils.assertTupleVectorsEquals(expected, actual.getTable(0));
    }
}
