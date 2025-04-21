package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static org.junit.Assume.assumeNotNull;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.ComparisonExpression;

/** Test of {@link HashMatch} */
public class HashMatchTest extends AJoinTest
{
    @Override
    IPhysicalPlan createInnerJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias)
    {
        return new HashMatch(0, outer, inner, List.of(ce("col1")), List.of(ce("col3")), predicate, populateAlias, false, false);
    }

    @Override
    IPhysicalPlan createLeftJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias)
    {
        return new HashMatch(0, outer, inner, List.of(ce("col1")), List.of(ce("col3")), predicate, populateAlias, true, false);
    }

    @Override
    protected IPhysicalPlan createIndexInnerJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias)
    {
        return new HashMatch(0, outer, inner, List.of(ce("col1")), List.of(ce("col3")), predicate, populateAlias, false, true);
    }

    @Override
    protected IPhysicalPlan createIndexLeftJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias)
    {
        return new HashMatch(0, outer, inner, List.of(ce("col1")), List.of(ce("col3")), predicate, populateAlias, true, true);
    }

    @Test
    public void test_join_integer_against_string_hash_outer()
    {
        Schema outerSchema = Schema.of(Column.of("col1", Type.Int));
        Schema innerSchema = Schema.of(Column.of("col3", Type.String));

        //@formatter:off
        List<TupleVector> outer = asList(
                TupleVector.of(outerSchema, asList(vv(Type.Int, 0, 1, 2)))
                );
        List<TupleVector> inner = asList(
                TupleVector.of(innerSchema, asList(vv(Type.String, UTF8String.from("2"), UTF8String.from("2"), null, null))));

        IDatasource dsOuter = schemaLessDS(() -> {}, false, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> {}, false, inner.toArray(new TupleVector[0]));
        //@formatter:on

        IExpression predicate = new ComparisonExpression(IComparisonExpression.Type.EQUAL, ce("col1"), ce("col3"));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Int, 2, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.String, UTF8String.from("2"), UTF8String.from("2")), actual.getColumn(1));
    }

    @Test
    public void test_join_string_against_integer_hash_outer()
    {
        Schema outerSchema = Schema.of(Column.of("col1", Type.String));
        Schema innerSchema = Schema.of(Column.of("col3", Type.Int));

        //@formatter:off
        List<TupleVector> outer = asList(
                TupleVector.of(outerSchema, asList(vv(Type.String, UTF8String.from("0"), UTF8String.from("1"), UTF8String.from("2"))))
                );
        List<TupleVector> inner = asList(
                TupleVector.of(innerSchema, asList(vv(Type.Int, 2, 2, null, null))));

        IDatasource dsOuter = schemaLessDS(() -> {}, false, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> {}, false, inner.toArray(new TupleVector[0]));
        //@formatter:on

        IExpression predicate = new ComparisonExpression(IComparisonExpression.Type.EQUAL, ce("col1"), ce("col3"));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.String, UTF8String.from("2"), UTF8String.from("2")), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Int, 2, 2), actual.getColumn(1));
    }

    @Test
    public void test_join_integer_against_string_hash_inner()
    {
        Schema outerSchema = Schema.of(Column.of("col1", Type.Int));
        Schema innerSchema = Schema.of(Column.of("col3", Type.String));

        //@formatter:off
        List<TupleVector> outer = asList(
                TupleVector.of(outerSchema, asList(vv(Type.Int, 0, 1, 2)))
                );
        List<TupleVector> inner = asList(
                TupleVector.of(innerSchema, asList(vv(Type.String, UTF8String.from("2"), UTF8String.from("2")))));

        IDatasource dsOuter = schemaLessDS(() -> {}, false, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> {}, false, inner.toArray(new TupleVector[0]));
        //@formatter:on

        IExpression predicate = new ComparisonExpression(IComparisonExpression.Type.EQUAL, ce("col1"), ce("col3"));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Int, 2, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.String, UTF8String.from("2"), UTF8String.from("2")), actual.getColumn(1));
    }

    @Test
    public void test_join_string_against_integer__hash_inner()
    {
        Schema outerSchema = Schema.of(Column.of("col1", Type.String));
        Schema innerSchema = Schema.of(Column.of("col3", Type.Int));

        //@formatter:off
        List<TupleVector> outer = asList(
                TupleVector.of(outerSchema, asList(vv(Type.String, UTF8String.from("2"), UTF8String.from("2"))))
                );
        List<TupleVector> inner = asList(
                TupleVector.of(innerSchema, asList(vv(Type.Int, 0, 1, 2))));

        IDatasource dsOuter = schemaLessDS(() -> {}, false, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> {}, false, inner.toArray(new TupleVector[0]));
        //@formatter:on

        IExpression predicate = new ComparisonExpression(IComparisonExpression.Type.EQUAL, ce("col1"), ce("col3"));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.String, UTF8String.from("2"), UTF8String.from("2")), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Int, 2, 2), actual.getColumn(1));
    }

    /** Empty second batch to verify regression when we tried to execute the inner with an outer empty tuple vector that is not allowed. */
    @Test
    public void test_indexed_inner_join_no_outer_rows_second_batch()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        //@formatter:off
        List<TupleVector> outer = asList(
                TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1, 2), vv(Type.Any, 4, 5, 6))),
                TupleVector.of(outerSchema, asList(vv(Type.Any), vv(Type.Any)))
                );
        List<TupleVector> inner = asList(
                TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3))));
        //@formatter:on

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), false, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), false, inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createIndexInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 4, 5), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(3));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    /** Verify that we don't return an empty Tuple Vector when we have full match of all outer rows. (Outer hash mode) */
    @Test
    public void test_left_join_all_outer_matches_outer_hash()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        //@formatter:off
        List<TupleVector> outer = asList(
                TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 4, 5)))
                );
        List<TupleVector> inner = asList(
                TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1, 2), vv(Type.Any, 2, 3, 4))));
        //@formatter:on

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), false, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), false, inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);

        while (it.hasNext())
        {
            TupleVector actual = it.next();

            assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

            assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 4, 5), actual.getColumn(1));
            assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(2));
            assertVectorsEquals(vv(Type.Any, 2, 3), actual.getColumn(3));

            assertTrue(!it.hasNext());
        }
        it.close();

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    /** Verify that we don't return an empty Tuple Vector when we have full match of all outer rows. (Inner hash mode) */
    @Test
    public void test_left_join_all_outer_matches_inner_hash()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        //@formatter:off
        List<TupleVector> outer = asList(
                TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1, 1), vv(Type.Any, 4, 5, 6)))
                );
        List<TupleVector> inner = asList(
                TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3))));
        //@formatter:on

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), false, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), false, inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);

        while (it.hasNext())
        {
            TupleVector actual = it.next();

            assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

            assertVectorsEquals(vv(Type.Any, 0, 1, 1), actual.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 4, 5, 6), actual.getColumn(1));
            assertVectorsEquals(vv(Type.Any, 0, 1, 1), actual.getColumn(2));
            assertVectorsEquals(vv(Type.Any, 2, 3, 3), actual.getColumn(3));

            assertTrue(!it.hasNext());
        }
        it.close();

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_schema_less_no_hashed_values()
    {
        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, null, null, null, null), vv(Type.Any, null, 2, 3, 10))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1, 2, 3), vv(Type.Any, 1, 2, 2, 4))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, null, null, null, null), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, null, 2, 3, 10), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null, null, null), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_schema_less_a_few_hashed_values()
    {
        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, null, null, 0, null), vv(Type.Any, null, 2, 3, 10))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1, 2, 3), vv(Type.Any, 1, 2, 2, 4))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, 0, null, null, null), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 3, null, 2, 10), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, null, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, null, null, null), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_schema_less_no_hashed_values_multiple_vectors()
    {
        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, null, null, null, null), vv(Type.Any, null, 2, 3, 10))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, null, null, null, null), vv(Type.Any, null, 20, 30, 101))), TupleVector.EMPTY,
                TupleVector.of(outerSchema, asList(vv(Type.Any, (Integer) null), vv(Type.Any, 100))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1, 2, 3), vv(Type.Any, 1, 2, 2, 4))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), true, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), true, inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, null, null, null, null, null, null, null, null, null), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, null, 2, 3, 10, null, 20, 30, 101, 100), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null, null, null, null, null, null, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null, null, null, null, null, null, null, null), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_schema_less_no_hashed_values_smaller_inner()
    {
        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, null, null, null, null), vv(Type.Any, null, 2, 3, 10))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1, 2), vv(Type.Any, 1, 2, 2))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, null, null, null, null), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, null, 2, 3, 10), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null, null, null), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_inner_join_populate_schema_less_smaller_inner()
    {
        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 0, 1, null), vv(Type.Any, 1, 2, 3, 10))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess, "a"), plan.getSchema());

        TupleIterator it = plan.execute(context);

        assertTrue(it.hasNext());
        assertTrue(it.hasNext());
        TupleVector actual = it.next();
        try
        {
            it.next();
            fail("Should fail with no such element");
        }
        catch (NoSuchElementException e)
        {
        }
        it.close();

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));

        // Verify inner tables
        TupleVector innerActual = actual.getColumn(2)
                .getTable(0);

        assertVectorsEquals(vv(Type.Any, 0), innerActual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1), innerActual.getColumn(1));

        innerActual = actual.getColumn(2)
                .getTable(1);

        assertVectorsEquals(vv(Type.Any, 0), innerActual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1), innerActual.getColumn(1));

        innerActual = actual.getColumn(2)
                .getTable(2);

        assertVectorsEquals(vv(Type.Any, 1), innerActual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 2), innerActual.getColumn(1));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_schema_less_smaller_inner()
    {
        /*
         * @fomatter:off
         * 
         * col1 0 0 1
         * col2 1 2 3
         * 
         * col3 0 1
         * col4 1 2
         * 
         * cartesian
         * 
         * col1 0 0 1 0 0 1
         * col2 1 2 3 1 2 3
         * col3 0 1 0 1 0 1
         * col4 1 2 1 2 1 2
         * ----------------
         * res  1 0 0 0 1 1   <-- join result
         * 
         *      
         * 
         * @formatter:on
         */

        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 0, 1, 2, 3), vv(Type.Any, 1, 2, 3, 4, 5))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        assertTrue(it.hasNext());
        TupleVector actual = it.next();
        assertTrue(it.hasNext());
        TupleVector actualNonMatched = it.next();
        assertFalse(it.hasNext());
        it.close();

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 1, 2), actual.getColumn(3));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actualNonMatched.getSchema());

        assertVectorsEquals(vv(Type.Any, 2, 3), actualNonMatched.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 5), actualNonMatched.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actualNonMatched.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actualNonMatched.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_schema_less_multiple_outer_smaller_inner_all_nulls()
    {
        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1, 2, 3), vv(Type.Any, 3, 4, 5))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, null, null), vv(Type.Any, 1, 2))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        assertTrue(it.hasNext());
        TupleVector actual = it.next();
        assertTrue(it.hasNext());
        TupleVector actualNonMatched = it.next();
        assertFalse(it.hasNext());
        it.close();

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, 0, 0), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actualNonMatched.getSchema());

        assertVectorsEquals(vv(Type.Any, 1, 2, 3), actualNonMatched.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 3, 4, 5), actualNonMatched.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null, null), actualNonMatched.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null, null), actualNonMatched.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_inner_join_no_populate_schema_less_smaller_inner_same_hash_different_value()
    {
        TestObject a = new TestObject(10, 10);
        TestObject b = new TestObject(20, 20);
        TestObject c = new TestObject(20, 30); // Equal to b but different hash

        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, a, b, c), vv(Type.Any, 1, 2, 3))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, b, c), vv(Type.Any, 1, 2))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);

        assertTrue(it.hasNext());
        assertTrue(it.hasNext());
        TupleVector actual = it.next();
        try
        {
            it.next();
            fail("Should fail with no such element");
        }
        catch (NoSuchElementException e)
        {
        }
        it.close();

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, b, c), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 2, 3), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, b, c), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    record TestObject(int hash, int value) implements Comparable<TestObject>
    {
        @Override
        public int hashCode()
        {
            return hash;
        }

        @Override
        public boolean equals(Object obj)
        {
            return obj instanceof TestObject that
                    && value == that.value;
        }

        @Override
        public int compareTo(TestObject o)
        {
            return Integer.compare(value, o.value);
        }
    }

    @Test
    public void test_left_join_no_populate_schema_less_same_hash_different_value()
    {
        TestObject a = new TestObject(10, 10);
        TestObject b = new TestObject(20, 20);
        TestObject c = new TestObject(20, 30); // Equal to b but different hash
        TestObject f = new TestObject(50, 50);

        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, a, b, c), vv(Type.Any, 1, 3, 10))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, a, b, f), vv(Type.Any, 1, 2, 4))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        assertTrue(it.hasNext());
        TupleVector actual = it.next();

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, a, b), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 3), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, a, b), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(3));

        assertTrue(it.hasNext());
        actual = it.next();
        assertVectorsEquals(vv(Type.Any, c), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 10), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, (Integer) null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, (Integer) null), actual.getColumn(3));

        assertFalse(it.hasNext());
        it.close();

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_inner_join_populate_schema_less_same_hash_different_value()
    {
        TestObject a = new TestObject(10, 10);
        TestObject b = new TestObject(20, 20);
        TestObject c = new TestObject(20, 30); // Equal to b but different hash
        TestObject f = new TestObject(50, 50);

        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, a, b, c), vv(Type.Any, 1, 3, 10))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, a, b, f), vv(Type.Any, 1, 2, 4))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess, "a"), plan.getSchema());

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, a, b), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 3), actual.getColumn(1));

        TupleVector innerActual = actual.getColumn(2)
                .getTable(0);

        assertVectorsEquals(vv(Type.Any, a), innerActual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1), innerActual.getColumn(1));

        innerActual = actual.getColumn(2)
                .getTable(1);

        assertVectorsEquals(vv(Type.Any, b), innerActual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 2), innerActual.getColumn(1));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Override
    @Test
    public void test_left_join_no_populate_with_push_outer_reference_mixed_sizes_multiple_outer_vectors_no_matches_in_middle_and_last_batch()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        //@formatter:off
        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2), vv(Type.Any, 4, 5))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 100, 200), vv(Type.Any, 400, 500))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 6))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1000, 2000), vv(Type.Any, 4000, 5000))));
        //@formatter:on
        IDatasource dsInner = schemaLessDS(() ->
        {
            innerClosed.incrementAndGet();
        }, ctx ->
        {
            // No matching rows in the middle batch and last batch
            if (innerClosed.get() == 1
                    || innerClosed.get() == 3)
            {
                return new TupleVector[0];
            }

            ValueVector col1 = innerClosed.get() == 0 ? vv(Type.Any, 0)
                    : vv(Type.Any, 1, 2);
            ValueVector col2 = innerClosed.get() == 0 ? vv(Type.Any, 10)
                    : vv(Type.Any, 20, 20);

            return new TupleVector[] { new TupleVector()
            {
                @Override
                public int getRowCount()
                {
                    return innerClosed.get() == 0 ? 1
                            : 2;
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    return column == 0 ? col1
                            : col2;
                }

                @Override
                public Schema getSchema()
                {
                    return innerSchema;
                }
            } };
        });

        IPhysicalPlan plan = createIndexLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);
        assumeNotNull(plan);

        TupleIterator it = plan.execute(context);
        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        assertTrue(it.hasNext());
        TupleVector actual = it.next();
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 10), actual.getColumn(3));

        assertTrue(it.hasNext());
        actual = it.next();
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 5), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, (Integer) null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, (Integer) null), actual.getColumn(3));

        assertTrue(it.hasNext());
        actual = it.next();
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 100, 200), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 400, 500), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertTrue(it.hasNext());
        actual = it.next();
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 6), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 20), actual.getColumn(3));

        assertTrue(it.hasNext());
        actual = it.next();
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 1000, 2000), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4000, 5000), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertFalse(it.hasNext());
        it.close();

        assertEquals(4, innerClosed.get());
        assertEquals(1, outerClosed.get());
    }

    @Override
    @Test
    public void test_left_join_no_populate_with_push_outer_reference_mixed_sizes_multiple_outer_vectors_no_matches_in_middle_and_first_batch()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        //@formatter:off
        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 100, 200), vv(Type.Any, 400, 500))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2), vv(Type.Any, 4, 5))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1000, 2000), vv(Type.Any, 4000, 5000))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 6))));
        //@formatter:on
        IDatasource dsInner = schemaLessDS(() ->
        {
            innerClosed.incrementAndGet();
        }, ctx ->
        {
            // No matching rows in the middle batch and last batch
            if (innerClosed.get() == 0
                    || innerClosed.get() == 2)
            {
                return new TupleVector[0];
            }

            ValueVector col1 = innerClosed.get() == 1 ? vv(Type.Any, 0)
                    : vv(Type.Any, 1, 2);
            ValueVector col2 = innerClosed.get() == 1 ? vv(Type.Any, 10)
                    : vv(Type.Any, 20, 20);

            return new TupleVector[] { new TupleVector()
            {
                @Override
                public int getRowCount()
                {
                    return innerClosed.get() == 1 ? 1
                            : 2;
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    return column == 0 ? col1
                            : col2;
                }

                @Override
                public Schema getSchema()
                {
                    return innerSchema;
                }
            } };
        });

        IPhysicalPlan plan = createIndexLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);
        assumeNotNull(plan);

        TupleIterator it = plan.execute(context);
        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        assertTrue(it.hasNext());
        TupleVector actual = it.next();

        // We have no inner vector here so only outer schema is available
        assertEquals(outerSchema, actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 100, 200), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 400, 500), actual.getColumn(1));

        assertTrue(it.hasNext());
        actual = it.next();
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 10), actual.getColumn(3));

        assertTrue(it.hasNext());
        actual = it.next();
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 5), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, (Integer) null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, (Integer) null), actual.getColumn(3));

        assertTrue(it.hasNext());
        actual = it.next();
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 1000, 2000), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4000, 5000), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertTrue(it.hasNext());
        actual = it.next();
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 6), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 20), actual.getColumn(3));

        assertFalse(it.hasNext());
        it.close();

        assertEquals(4, innerClosed.get());
        assertEquals(1, outerClosed.get());
    }

    @Test
    public void test_left_join_populate_schema_less_same_hash_different_value_smaller_inner()
    {
        TestObject a = new TestObject(10, 10);
        TestObject b = new TestObject(20, 20);
        TestObject c = new TestObject(20, 30); // Equal to b but different hash

        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, a, b, c), vv(Type.Any, 1, 3, 10))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, b, c), vv(Type.Any, 1, 2))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess, "a"), plan.getSchema());

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        //@formatter:off
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, b, c, a), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 3, 10, 1), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchema),
                TupleVector.of(innerSchema, asList(
                        vv(Type.Any, b),
                        vv(Type.Any, 1)
                        )),
                TupleVector.of(innerSchema, asList(
                        vv(Type.Any, c),
                        vv(Type.Any, 2)
                        )),
                null
                ), actual.getColumn(2));
        //@formatter:on

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_inner_join_no_populate_multiple_outer_vectors_smaller_inner()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        //@formatter:off
        List<TupleVector> outer = asList(
                TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1, 2), vv(Type.Any, 4, 5, 6))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 10), vv(Type.Any, 40, 50))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 20), vv(Type.Any, 70, 90))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 3, 4), vv(Type.Any, 7, 8))));
        //@formatter:on
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3))));

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), false, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), false, inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 0, 1, 0, 0, 0, 0), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 4, 5, 40, 40, 70, 70), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 1, 0, 0, 0, 0), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3, 1, 2, 1, 2), actual.getColumn(3));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    @Test
    public void test_inner_join_no_populate_multiple_inner_vectors_smaller_outer()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3))));
        //@formatter:off
        List<TupleVector> inner = asList(
                TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1, 2), vv(Type.Any, 4, 5, 6))),
                TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 10), vv(Type.Any, 40, 50))),
                TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 20), vv(Type.Any, 70, 90))),
                TupleVector.of(innerSchema, asList(vv(Type.Any, 3, 4), vv(Type.Any, 7, 8))));
        //@formatter:on

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), false, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), false, inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 0, 1, 0, 0, 0, 0), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3, 1, 2, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 1, 0, 0, 0, 0), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 4, 4, 5, 40, 40, 70, 70), actual.getColumn(3));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_multiple_outer_vectors_smaller_inner()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1, 2), vv(Type.Any, 4, 5, 6))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 3, 4), vv(Type.Any, 7, 8))));
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3))));

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), false, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), false, inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 0, 1, 2, 3, 4), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 4, 5, 6, 7, 8), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 1, null, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3, null, null, null), actual.getColumn(3));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }
}
