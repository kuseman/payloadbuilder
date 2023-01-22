package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.List;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.BiFunction;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.ColumnReference;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.ComparisonExpression;

/** Base class for testing joins */
public abstract class AJoinTest extends APhysicalPlanTest
{
    /** Create a physical plan to test for inner join implementations */
    abstract IPhysicalPlan createInnerJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias);

    /** Create a physical plan to test for left join implementations */
    abstract IPhysicalPlan createLeftJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias);

    protected Schema outerSchema = Schema.of(CoreColumn.of(table.column("col1"), ResolvedType.of(Type.Any)), CoreColumn.of(table.column("col2"), ResolvedType.of(Type.Any)));
    protected Schema innerSchema = Schema.of(CoreColumn.of(tableB.column("col3"), ResolvedType.of(Type.Any)), CoreColumn.of(tableB.column("col4"), ResolvedType.of(Type.Any)));

    protected Schema outerSchemaLess = Schema.of(CoreColumn.of(new ColumnReference(table, "t", ColumnReference.Type.ASTERISK), ResolvedType.of(Type.Any)));
    protected Schema innerSchemaLess = Schema.of(CoreColumn.of(new ColumnReference(tableB, "b", ColumnReference.Type.ASTERISK), ResolvedType.of(Type.Any)));

    // outer.col1 = inner.col1
    protected IExpression predicate = new ComparisonExpression(IComparisonExpression.Type.EQUAL, ce("col1"), ce("col3"));

    // ---------------------- No populate

    @Test
    public void test_inner_join_no_populate_single_vectors_schema_less()
    {
        assertInnerJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    public void test_left_join_no_populate_single_vectors_schema_less()
    {
        assertLeftJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    public void test_inner_join_no_populate_multiple_outer_vectors_schema_less()
    {
        assertInnerJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    public void test_left_join_no_populate_multiple_outer_vectors_schema_less()
    {
        assertLeftJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))), TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    public void test_inner_join_no_populate_multiple_inner_vectors_schema_less()
    {
        assertInnerJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    public void test_left_join_no_populate_multiple_inner_vectors_schema_less()
    {
        assertLeftJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    public void test_inner_join_no_populate_multiple_outer_and_inner_vectors_schema_less()
    {
        assertInnerJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    public void test_left_join_no_populate_multiple_outer_and_inner_schema_less()
    {
        assertLeftJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))), TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    // ---------------------- Populate

    @Test
    public void test_inner_join_populate_single_vectors_schema_less()
    {
        assertInnerJoinWithPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    public void test_left_join_populate_single_vectors_schema_less()
    {
        assertLeftJoinPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    public void test_inner_join_populate_multiple_outer_vectors_schema_less()
    {
        assertInnerJoinWithPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    public void test_left_join_populate_multiple_outer_vectors_schema_less()
    {
        assertLeftJoinPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))), TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    public void test_inner_join_populate_multiple_inner_vectors_schema_less()
    {
        assertInnerJoinWithPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    public void test_left_join_populate_multiple_inner_vectors_schema_less()
    {
        assertLeftJoinPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    public void test_inner_join_populate_multiple_outer_and_inner_vectors_schema_less()
    {
        assertInnerJoinWithPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    public void test_left_join_populate_multiple_outer_and_inner_schema_less()
    {
        assertLeftJoinPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))), TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    public void test_inner_join_no_populate_verify_children_are_closed_with_break()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scan(dsOuter, table, outerSchema, 1), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        it.close();

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_inner_join_populate_verify_children_are_closed_with_break()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scan(dsOuter, table, outerSchema, 1), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        TupleIterator it = plan.execute(context);

        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        it.close();

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_inner_join_no_populate_no_outer_vectors()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), new TupleVector[0]);
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 10, 10, 11), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        // Inner should not be opened
        assertFalse(innerClosed.get());
    }

    @Test
    public void test_inner_join_no_populate_no_outer_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.EMPTY);
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 10, 10, 11), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        // Inner should not be opened
        assertFalse(innerClosed.get());
    }

    @Test
    public void test_inner_join_populate_no_outer_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), new TupleVector[0]);
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 10, 10, 11), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        // Inner should not be opened
        assertFalse(innerClosed.get());
    }

    @Test
    public void test_inner_join_no_populate_no_inner_vectors()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createInnerJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_inner_join_no_populate_no_inner_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.EMPTY);

        IPhysicalPlan plan = createInnerJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_no_inner_rows_schema_less()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createLeftJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), plan.execute(context));

        assertEquals(SchemaUtils.concat(outerSchema, innerSchema), actual.getSchema());

        assertEquals(2, actual.getRowCount());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_no_inner_rows_verify_plan_and_runtime_schema_with_asterisk_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createLeftJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.concat(outerSchemaLess, innerSchemaLess), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), plan.execute(context));

        assertEquals(outerSchema, actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_outer_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createLeftJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.concat(outerSchema, innerSchemaLess), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), plan.execute(context));

        assertEquals(outerSchema, actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_inner_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createLeftJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.concat(outerSchemaLess, innerSchema), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), plan.execute(context));

        // Runtime schema is full concat of inner and outer since we have a static inner
        assertEquals(SchemaUtils.concat(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_no_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_outer_and_inner_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createLeftJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        assertEquals(SchemaUtils.concat(outerSchema, innerSchema), plan.getSchema());
        assertFalse(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), plan.execute(context));

        // Runtime schema is full concat of inner and outer since we have a static inner
        assertEquals(SchemaUtils.concat(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_with_populate_no_inner_rows_verify_plan_and_runtime_schema_with_asterisk_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createLeftJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        assertEquals(SchemaUtils.populate(outerSchemaLess, "a", innerSchemaLess), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), plan.execute(context));

        // Populate always have a concated schema
        assertEquals(SchemaUtils.populate(outerSchema, "a", innerSchemaLess), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchemaLess), null, null), actual.getColumn(2));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_with_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_outer_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createLeftJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        assertEquals(SchemaUtils.populate(outerSchema, "a", innerSchemaLess), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), plan.execute(context));

        // Populate always have a concated schema
        assertEquals(SchemaUtils.populate(outerSchema, "a", innerSchemaLess), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchemaLess), null, null), actual.getColumn(2));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_with_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_inner_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createLeftJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        assertEquals(SchemaUtils.populate(outerSchemaLess, "a", innerSchema), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), plan.execute(context));

        // Populate always have a concated schema
        assertEquals(SchemaUtils.populate(outerSchema, "a", innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchema), null, null), actual.getColumn(2));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_left_join_with_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_outer_and_inner_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createLeftJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        assertEquals(SchemaUtils.populate(outerSchema, "a", innerSchema), plan.getSchema());
        assertFalse(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), plan.execute(context));

        // Populate always have a concated schema
        assertEquals(SchemaUtils.populate(outerSchema, "a", innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchema), null, null), actual.getColumn(2));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_inner_join_populate_no_inner_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createInnerJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_inner_join_no_populate_no_matching_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 10, 10, 11), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    public void test_inner_join_populate_no_matching_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 10, 10, 11), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    // @Ignore("Not correct test, there can be a schema when one side is empty. Ie. constant scan")
    // @Test
    // public void test_no_schema_when_one_input_is_schema_less()
    // {
    // IDatasource dsOuter;
    // IDatasource dsInner;
    // IPhysicalPlan plan;
    //
    // dsOuter = schemaLessDS(() ->
    // {
    // }, TupleVector.of(outerSchema, asList(vv(Type.Object, 0, 1), vv(Type.Object, 1, 2))));
    // dsInner = schemaDS(() ->
    // {
    // }, new TupleVector[0]);
    // plan = createInnerJoin(scan(dsOuter), scan(dsInner), (tv, ctx) -> predicate.eval(tv, ctx), true);
    //
    // assertEquals(Schema.EMPTY, plan.getSchema());
    //
    // dsOuter = schemaDS(() ->
    // {
    // }, TupleVector.of(outerSchema, asList(vv(Type.Object, 0, 1), vv(Type.Object, 1, 2))));
    // dsInner = schemaLessDS(() ->
    // {
    // }, new TupleVector[0]);
    // plan = createInnerJoin(scan(dsOuter), scan(dsInner), (tv, ctx) -> predicate.eval(tv, ctx), true);
    //
    // assertEquals(Schema.EMPTY, plan.getSchema());
    //
    // dsOuter = schemaLessDS(() ->
    // {
    // }, TupleVector.of(outerSchema, asList(vv(Type.Object, 0, 1), vv(Type.Object, 1, 2))));
    // dsInner = schemaLessDS(() ->
    // {
    // }, new TupleVector[0]);
    // plan = createInnerJoin(scan(dsOuter), scan(dsInner), (tv, ctx) -> predicate.eval(tv, ctx), true);
    //
    // assertEquals(Schema.EMPTY, plan.getSchema());
    // }

    private void assertInnerJoinWithPopulateSchemaLess(List<TupleVector> outer, List<TupleVector> inner)
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createInnerJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.populate(outerSchemaLess, "a", innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), it);

        assertEquals(SchemaUtils.populate(outerSchema, "a", innerSchema), actual.getSchema());
        assertEquals(2, actual.getRowCount());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));

        ValueVector vv = actual.getColumn(2);
        assertEquals(ResolvedType.table(innerSchema), vv.type());

        assertEquals(2, vv.size());

        actual = vv.getTable(0);
        assertEquals(2, actual.getSchema()
                .getSize());
        assertEquals(2, actual.getRowCount());

        assertVectorsEquals(vv(Type.Any, 0, 0), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));

        actual = vv.getTable(1);
        assertEquals(2, actual.getSchema()
                .getSize());
        assertEquals(1, actual.getRowCount());

        assertVectorsEquals(vv(Type.Any, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 3), actual.getColumn(1));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    private void assertInnerJoinNoPopulateSchemaLess(List<TupleVector> outer, List<TupleVector> inner)
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createInnerJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.concat(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), it);

        assertEquals(SchemaUtils.concat(outerSchema, innerSchema), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    private void assertLeftJoinNoPopulateSchemaLess(List<TupleVector> outer, List<TupleVector> inner)
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.concat(outerSchemaLess, innerSchemaLess), plan.getSchema());

        /*
         * @formatter:off
         * A schema less left join should return these rows in one ore more vectors
         * Since we cannot concat vectors with different schemas a manual
         * assert has to be made
         * 
         * Two vectors:
         * col1 col2 col3 col4
         * 0    4    0    1
         * 0    4    0    2
         * 1    6    1    3
         * 
         * col1 col2
         * 2    5
         * 
         * One vector:
         * col1 col2 col3 col4
         * 0    4    0    1
         * 0    4    0    2
         * 1    6    1    3
         * 2    5    null null
         * 
         * @formatter:on
         */

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), it);

        assertEquals(SchemaUtils.concat(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 0, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 4, 6, 5), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 1, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3, null), actual.getColumn(3));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    private void assertLeftJoinPopulateSchemaLess(List<TupleVector> outer, List<TupleVector> inner)
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        // Compile time schema
        assertEquals(SchemaUtils.populate(outerSchemaLess, "a", innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context.getBufferAllocator(), it);

        // Runtime schema
        assertEquals(SchemaUtils.populate(outerSchema, "a", innerSchema), actual.getSchema());

        /* @formatter:off
         * 
         * Outer
         *   col1 0 2 1
         *   col2 4 5 6
         * 
         * Inner
         *   col3 0 0 1
         *   col4 1 2 3
         * 
         * col1  0 0 0 2 2 2 1 1 1
         * col2  4 4 4 5 5 5 6 6 6
         * col3  0 0 1 0 0 1 0 0 1
         * col4  1 2 3 1 2 3 1 2 3
         * -----------------------
         *       1 1 0 0 0 0 0 0 1
         * 
         * @formatter:on
     */

        assertVectorsEquals(vv(Type.Any, 0, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 6, 5), actual.getColumn(1));

        ValueVector vv = actual.getColumn(2);
        assertEquals(ResolvedType.table(innerSchema), vv.type());
        assertEquals(3, vv.size());

        // Assert populated vectors
        TupleVector tv = vv.getTable(0);
        assertEquals(2, tv.getRowCount());
        assertEquals(innerSchema, tv.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 0), tv.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), tv.getColumn(1));

        tv = vv.getTable(1);
        assertEquals(1, tv.getRowCount());
        assertEquals(innerSchema, tv.getSchema());

        assertVectorsEquals(vv(Type.Any, 1), tv.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 3), tv.getColumn(1));

        // Non matched row
        assertTrue(vv.isNull(2));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

}
