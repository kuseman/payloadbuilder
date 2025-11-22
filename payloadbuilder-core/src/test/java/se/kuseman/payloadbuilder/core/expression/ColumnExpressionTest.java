package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;

/** Test of {@link ColumnExpression} */
class ColumnExpressionTest extends APhysicalPlanTest
{
    @Test
    void test_illegal_args_no_ordinal_no_path_no_asterisk()
    {
        TableSourceReference tableSource = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "t");
        assertThrows(IllegalArgumentException.class, () -> ColumnExpression.Builder.of("col", ResolvedType.of(Type.Any))
                .withColumnReference(new ColumnReference("col", tableSource, Column.MetaData.EMPTY))
                .build());
    }

    @Test
    void test_ordinal_no_column_reference()
    {
        // CSOFF
        ColumnExpression e;
        ValueVector actual;
        TupleVector tv;
        // CSON

        e = ce("col", 0, ResolvedType.of(Type.Int));
        tv = TupleVector.of(Schema.of(col("a", Type.Int)), asList(vv(Type.Int, 0, 0)));

        assertEquals(QualifiedName.of("col"), e.getQualifiedColumn());
        assertEquals(Type.Int, e.getType()
                .getType());
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(Type.Int, 0, 0), actual);

        Schema innerSchema = Schema.of(col("b", Type.Int));
        Schema schema = Schema.of(new Column("a", ResolvedType.table(innerSchema)));

        // Test nested tuple vector, this will return a vector of value vectors
        e = ce("a", 0, ResolvedType.of(Type.Int));
        //@formatter:off
        tv = TupleVector.of(schema, asList(
                vv(ResolvedType.table(innerSchema),
                        TupleVector.of(innerSchema, asList(vv(Type.Int, 4, 5))),
                        TupleVector.of(innerSchema, asList(vv(Type.Int, 6, 7))))));
        //@formatter:on
        assertEquals(Type.Int, e.getType()
                .getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.table(innerSchema), actual.type());
        assertEquals(2, actual.size());

        assertVectorsEquals(vv(ResolvedType.table(innerSchema), TupleVector.of(innerSchema, asList(vv(Type.Int, 4, 5))), TupleVector.of(innerSchema, asList(vv(Type.Int, 6, 7)))), actual);

        // Test nested map
        e = ce("a", 0);
        tv = TupleVector.of(Schema.of(col("a", Type.Any)), asList(vv(Type.Any, ofEntries(entry("b", 123)), null, ofEntries(entry("b", 456)))));

        assertEquals(ResolvedType.of(Type.Any), e.getType());
        actual = e.eval(tv, context);
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), ofEntries(entry("b", 123)), null, ofEntries(entry("b", 456))), actual);
    }

    @Test
    void test_column_no_column_reference()
    {
        // CSOFF
        ColumnExpression e;
        ValueVector actual;
        TupleVector tv;
        // CSON

        e = ce("col", ResolvedType.of(Type.Int));
        tv = TupleVector.of(Schema.of(col("col", Type.Int)), asList(vv(Type.Int, 0, 0)));

        assertEquals("col", e.getAlias()
                .getAlias());
        assertEquals(ResolvedType.of(Type.Int), e.getType());
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(Type.Int, 0, 0), actual);

        // Missing column
        e = ce("nono");
        assertEquals(Type.Any, e.getType()
                .getType());
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(Type.Any, null, null), actual);

        /*
         * @formatter:off
         *
         * a
         *   p0
         *     b: [1,2,3]
         *   p1
         *     b: [2]
         *   p2
         *     b: [5,8]
         *   p3
         *     b: [null]
         *
         * 'a.b' means we first fetch 'a' which gives us a tuple vector type vector
         * then we retrieve b from that and which means we return a value vector
         * or value vectors
         *
         * Result: [ [1,2,3], [2], [5,8], [null] ]
         *
         * a.b.map(x -> x + 10) =>
         *   [ [11,12,13], [12], [15,18], [null] ]
         *
         *
         * @formatter:on
         */

        Schema innerSchema = Schema.of(col("b", Type.Int));
        Schema schema = Schema.of(new Column("a", ResolvedType.table(innerSchema)));

        // Test nested tuple vector, this will return a vector of value vectors
        e = ce("a", ResolvedType.table(innerSchema));

        assertEquals(ResolvedType.table(innerSchema), e.getType());
        assertEquals(innerSchema, e.getType()
                .getSchema());

        //@formatter:off
        actual = e.eval(TupleVector.of(schema, asList(
                vv(ResolvedType.table(innerSchema),
                        TupleVector.of(innerSchema, asList(vv(Type.Int, 4, 5))),
                        TupleVector.of(innerSchema, asList(vv(Type.Int, 6, 7)))))), context);
        //@formatter:on
        assertEquals(ResolvedType.table(innerSchema), actual.type());
        assertEquals(2, actual.size());

        assertVectorsEquals(vv(ResolvedType.table(innerSchema), TupleVector.of(innerSchema, asList(vv(Type.Int, 4, 5))), TupleVector.of(innerSchema, asList(vv(Type.Int, 6, 7)))), actual);

        // Test nested map
        e = ce("a");
        //@formatter:off
        actual = e.eval(TupleVector.of(Schema.of(col("a", Type.Any)), asList(
                vv(Type.Any, ofEntries(entry("b", 123)), null, ofEntries(entry("b", 456))))), context);
        //@formatter:on
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), ofEntries(entry("b", 123)), null, ofEntries(entry("b", 456))), actual);
    }

    @Test
    void test_column_with_table_reference()
    {
        // CSOFF
        ColumnExpression e;
        ValueVector actual;
        TupleVector tv;
        // CSON

        TableSourceReference tableSource = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "a");
        TableSourceReference tableSourceB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");
        e = cre("col", tableSourceB, ResolvedType.of(Type.Int));
        tv = TupleVector.of(Schema.of(col("col", ResolvedType.of(Type.Int), tableSource)), asList(vv(Type.Int, 0, 0)));

        assertEquals("col", e.getAlias()
                .getAlias());

        // Not the table source the expression is looking for => null values
        assertEquals(Type.Int, e.getType()
                .getType());
        actual = e.eval(tv, context);
        assertVectorsEquals(ValueVector.literalNull(ResolvedType.of(Type.Any), 2), actual);

        // Column has no table source
        tv = TupleVector.of(Schema.of(col("col", Type.Int)), asList(vv(Type.Int, 0, 0)));
        assertEquals(Type.Int, e.getType()
                .getType());
        actual = e.eval(tv, context);
        assertVectorsEquals(ValueVector.literalNull(ResolvedType.of(Type.Any), 2), actual);

        // Matching table source, non matching column
        tv = TupleVector.of(Schema.of(col("col2", ResolvedType.of(Type.Int), tableSourceB)), asList(vv(Type.Int, 0, 0)));
        assertEquals(Type.Int, e.getType()
                .getType());
        actual = e.eval(tv, context);
        assertVectorsEquals(ValueVector.literalNull(ResolvedType.of(Type.Any), 2), actual);

        // Matching table source
        tv = TupleVector.of(Schema.of(col("col", ResolvedType.of(Type.Int), tableSourceB)), asList(vv(Type.Int, 0, 0)));
        assertEquals(Type.Int, e.getType()
                .getType());
        actual = e.eval(tv, context);
        assertVectorsEquals(vv(Type.Int, 0, 0), actual);

        /*
         * @formatter:off
         *
         * a
         *   p0
         *     b: [1,2,3]
         *   p1
         *     b: [2]
         *   p2
         *     b: [5,8]
         *   p3
         *     b: [null]
         *
         * 'a.b' means we first fetch 'a' which gives us a tuple vector type vector
         * then we retrieve b from that and which means we return a value vector
         * or value vectors
         *
         * Result: [ [1,2,3], [2], [5,8], [null] ]
         *
         * a.b.map(x -> x + 10) =>
         *   [ [11,12,13], [12], [15,18], [null] ]
         *
         *
         * @formatter:on
         */

        Schema innerSchema = Schema.of(col("b", Type.Int));
        Schema schema = Schema.of(col("col", ResolvedType.table(innerSchema), tableSource));

        // Test nested tuple vector, this will return a vector of value vectors
        e = cre("col", tableSource, ResolvedType.array(Type.Any));
        //@formatter:off
        actual = e.eval(TupleVector.of(schema, asList(
                vv(ResolvedType.table(innerSchema),
                        TupleVector.of(innerSchema, asList(vv(Type.Int, 4, 5))),
                        TupleVector.of(innerSchema, asList(vv(Type.Int, 6, 7)))))), context);
        //@formatter:on
        assertEquals(ResolvedType.table(innerSchema), actual.type());
        assertEquals(2, actual.size());

        assertVectorsEquals(vv(ResolvedType.table(innerSchema), TupleVector.of(innerSchema, asList(vv(Type.Int, 4, 5))), TupleVector.of(innerSchema, asList(vv(Type.Int, 6, 7)))), actual);

        // Test nested map
        //@formatter:off
        actual = e.eval(TupleVector.of(Schema.of(col("col", ResolvedType.of(Type.Any), tableSource)), asList(
                vv(Type.Any, ofEntries(entry("b", 123)), null, ofEntries(entry("b", 456))))), context);
        //@formatter:on
        assertEquals(ResolvedType.of(Type.Any), actual.type());
        assertEquals(3, actual.size());

        assertVectorsEquals(vv(ResolvedType.of(Type.Any), ofEntries(entry("b", 123)), null, ofEntries(entry("b", 456))), actual);
    }
}
