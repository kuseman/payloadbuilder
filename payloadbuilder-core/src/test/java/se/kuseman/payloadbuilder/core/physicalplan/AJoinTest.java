package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;
import static org.junit.jupiter.api.Assumptions.assumeTrue;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.catalog.Column;
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
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.ComparisonExpression;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Base class for testing joins */
abstract class AJoinTest extends APhysicalPlanTest
{
    /** Create a physical plan to test for inner join implementations */
    abstract IPhysicalPlan createInnerJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias);

    /** Create a physical plan to test for left join implementations */
    abstract IPhysicalPlan createLeftJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias);

    protected Schema outerSchema = Schema.of(col("col1", ResolvedType.of(Type.Any), table), col("col2", ResolvedType.of(Type.Any), table));
    protected Schema innerSchema = Schema.of(col("col3", ResolvedType.of(Type.Any), tableB), col("col4", ResolvedType.of(Type.Any), tableB));

    /** Create a physical plan to test for inner join implementations with index */
    protected IPhysicalPlan createIndexInnerJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias)
    {
        return null;
    }

    /** Create a physical plan to test for left join implementations with index */
    protected IPhysicalPlan createIndexLeftJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias)
    {
        return null;
    }

    protected Schema outerSchemaLess = Schema.of(ast("t", table));
    protected Schema innerSchemaLess = Schema.of(ast("b", tableB));

    // outer.col1 = inner.col1
    protected IExpression predicate = new ComparisonExpression(IComparisonExpression.Type.EQUAL, ce("col1"), ce("col3"));

    // ---------------------- Index

    @Test
    void test_left_join_no_populate_and_push_outer_reference_verify_children_are_closed_with_break()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = new Limit(0, createIndexLeftJoin(scan(dsOuter, table, outerSchema, 1), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null), intLit(1));

        TupleIterator it = plan.execute(context);

        assertTrue(it.hasNext());
        assertEquals(1, it.next()
                .getRowCount());
        it.close();

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_and_push_outer_reference_verify_children_are_closed_with_break()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = new Limit(0, createIndexInnerJoin(scan(dsOuter, table, outerSchema, 1), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null), intLit(1));

        TupleIterator it = plan.execute(context);

        assertTrue(it.hasNext());
        assertEquals(1, it.next()
                .getRowCount());
        it.close();

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_populate_and_push_outer_reference_verify_children_are_closed_with_break()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = new Limit(0, createIndexInnerJoin(scan(dsOuter, table, outerSchema, 1), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "p"), intLit(1));

        TupleIterator it = plan.execute(context);

        assertTrue(it.hasNext());
        assertEquals(1, it.next()
                .getRowCount());
        it.close();

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_with_populate_and_push_outer_reference_no_outer_rows()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.EMPTY);
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.EMPTY);

        IPhysicalPlan plan = createIndexInnerJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "p");
        assumeTrue(plan != null);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, actual);

        assertEquals(1, outerClosed.get());
        // Inner should not be touched
        assertEquals(0, innerClosed.get());
    }

    @Test
    void test_inner_join_with_populate_and_push_outer_reference_no_inner_rows()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.EMPTY);

        IPhysicalPlan plan = createIndexInnerJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "p");
        assumeTrue(plan != null);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, actual);

        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    void test_left_join_with_populate_and_push_outer_reference_no_inner_rows()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.EMPTY);

        IPhysicalPlan plan = new Sort(0, createIndexLeftJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "p"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));
        assumeTrue(plan != null);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        //@formatter:off
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(SchemaUtils.joinSchema(outerSchema, innerSchemaLess, "p"),
                asList(
                        vv(Type.Any, 0, 1, 2),
                        vv(Type.Any, 4, 6, 5),
                        vv(ResolvedType.table(innerSchemaLess), null, null, null))), actual);
        //@formatter:on

        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    void test_inner_join_with_populate_and_push_outer_reference()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2), vv(Type.Any, 4, 5))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), ctx ->
        {
            // Return values based on outer
            final TupleVector outerTupleVector = ((StatementContext) ctx.getStatementContext()).getIndexSeekTupleVector();
            return new TupleVector[] { new TupleVector()
            {
                @Override
                public int getRowCount()
                {
                    return outerTupleVector.getRowCount();
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    final ValueVector col = outerTupleVector.getColumn(column);
                    // Leave join column as is
                    if (column == 0)
                    {
                        return col;
                    }

                    return new ValueVectorAdapter(col)
                    {
                        @Override
                        public Object getAny(int row)
                        {
                            return (((Integer) col.getAny(row)) + 1) * 10;
                        }
                    };
                }

                @Override
                public Schema getSchema()
                {
                    return innerSchema;
                }
            } };
        });

        IPhysicalPlan plan = createIndexInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "p");
        assumeTrue(plan != null);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess, "p"), plan.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 2, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 5, 6), actual.getColumn(1));

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.table(innerSchema),
                TupleVector.of(innerSchema, asList(
                        vv(Type.Any, 0),
                        vv(Type.Any, 50)
                        )),
                TupleVector.of(innerSchema, asList(
                        vv(Type.Any, 2),
                        vv(Type.Any, 60)
                        )),
                TupleVector.of(innerSchema, asList(
                        vv(Type.Any, 1),
                        vv(Type.Any, 70)
                        ))
                ), actual.getColumn(2));
        //@formatter:on

        assertEquals(3, actual.getRowCount());
        assertEquals(2, innerClosed.get());
    }

    @Test
    void test_inner_join_with_populate_and_push_outer_reference_smaller_inner()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2), vv(Type.Any, 4, 5))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1, 10), vv(Type.Any, 6, 70))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), ctx ->
        {
            // Return values based on outer
            final TupleVector outerTupleVector = ((StatementContext) ctx.getStatementContext()).getIndexSeekTupleVector();
            return new TupleVector[] { new TupleVector()
            {
                @Override
                public int getRowCount()
                {
                    return 1;
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    final ValueVector col = outerTupleVector.getColumn(column);
                    // Leave join column as is
                    if (column == 0)
                    {
                        return col;
                    }

                    return new ValueVectorAdapter(col)
                    {
                        @Override
                        public Object getAny(int row)
                        {
                            return (((Integer) col.getAny(row)) + 1) * 10;
                        }
                    };
                }

                @Override
                public Schema getSchema()
                {
                    return innerSchema;
                }
            } };
        });

        IPhysicalPlan plan = createIndexInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "p");
        assumeTrue(plan != null);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess, "p"), plan.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 6), actual.getColumn(1));

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.table(innerSchema),
                TupleVector.of(innerSchema, asList(
                        vv(Type.Any, 0),
                        vv(Type.Any, 50)
                        )),
                TupleVector.of(innerSchema, asList(
                        vv(Type.Any, 1),
                        vv(Type.Any, 70)
                        ))
                ), actual.getColumn(2));
        //@formatter:on

        assertEquals(2, actual.getRowCount());
        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    void test_inner_join_with_push_outer_reference()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), ctx ->
        {
            // Return values based on outer
            final TupleVector outerTupleVector = ((StatementContext) ctx.getStatementContext()).getIndexSeekTupleVector();
            return new TupleVector[] { new TupleVector()
            {
                @Override
                public int getRowCount()
                {
                    return outerTupleVector.getRowCount();
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    final ValueVector col = outerTupleVector.getColumn(column);
                    // Leave join column as is
                    if (column == 0)
                    {
                        return col;
                    }

                    return new ValueVectorAdapter(col)
                    {
                        @Override
                        public Object getAny(int row)
                        {
                            return (((Integer) col.getAny(row)) + 1) * 10;
                        }
                    };
                }

                @Override
                public Schema getSchema()
                {
                    return innerSchema;
                }
            } };
        });

        IPhysicalPlan plan = createIndexInnerJoin(scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);
        assumeTrue(plan != null);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 2, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 5, 6), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 2, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 50, 60, 70), actual.getColumn(3));

        assertEquals(3, actual.getRowCount());
        assertEquals(1, innerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_with_push_outer_reference()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), ctx ->
        {
            // Return values based on outer
            final TupleVector outerTupleVector = ((StatementContext) ctx.getStatementContext()).getIndexSeekTupleVector();
            return new TupleVector[] { new TupleVector()
            {
                @Override
                public int getRowCount()
                {
                    return outerTupleVector.getRowCount();
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    final ValueVector col = outerTupleVector.getColumn(column);
                    // Leave join column as is
                    if (column == 0)
                    {
                        return col;
                    }

                    return new ValueVectorAdapter(col)
                    {
                        @Override
                        public Object getAny(int row)
                        {
                            return (((Integer) col.getAny(row)) + 1) * 10;
                        }
                    };
                }

                @Override
                public Schema getSchema()
                {
                    return innerSchema;
                }
            } };
        });

        IPhysicalPlan plan = createIndexInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);
        assumeTrue(plan != null);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 2, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 5, 6), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 2, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 50, 60, 70), actual.getColumn(3));

        assertEquals(3, actual.getRowCount());
        assertEquals(1, innerClosed.get());
        assertEquals(1, outerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_with_push_outer_reference_smaller_inner()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), ctx ->
        {
            // Return values based on outer
            final TupleVector outerTupleVector = ((StatementContext) ctx.getStatementContext()).getIndexSeekTupleVector();
            return new TupleVector[] { new TupleVector()
            {
                @Override
                public int getRowCount()
                {
                    return 1;
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    final ValueVector col = outerTupleVector.getColumn(column);
                    // Leave join column as is
                    if (column == 0)
                    {
                        return col;
                    }

                    return new ValueVectorAdapter(col)
                    {
                        @Override
                        public Object getAny(int row)
                        {
                            return (((Integer) col.getAny(row)) + 1) * 10;
                        }
                    };
                }

                @Override
                public Schema getSchema()
                {
                    return innerSchema;
                }
            } };
        });

        IPhysicalPlan plan = createIndexInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);
        assumeTrue(plan != null);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        assertVectorsEquals(vv(Type.Any, 0), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 50), actual.getColumn(3));

        assertEquals(1, actual.getRowCount());
        assertEquals(1, innerClosed.get());
        assertEquals(1, outerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_with_push_outer_reference_mixed_sizes_multiple_outer_vectors()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2), vv(Type.Any, 4, 5))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), ctx ->
        {
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

        IPhysicalPlan plan = createIndexInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);
        assumeTrue(plan != null);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 6), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 10, 20), actual.getColumn(3));

        assertEquals(2, actual.getRowCount());
        assertEquals(2, innerClosed.get());
        assertEquals(1, outerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_with_push_outer_reference_multiple_outer_vectors_smaller_inner()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2), vv(Type.Any, 4, 5))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1, 2), vv(Type.Any, 6, 10))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), ctx ->
        {
            ValueVector col1 = innerClosed.get() == 0 ? vv(Type.Any, 0)
                    : vv(Type.Any, 1);
            ValueVector col2 = innerClosed.get() == 0 ? vv(Type.Any, 10)
                    : vv(Type.Any, 20);

            return new TupleVector[] { new TupleVector()
            {
                @Override
                public int getRowCount()
                {
                    return 1;
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

        IPhysicalPlan plan = createIndexInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null);
        assumeTrue(plan != null);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 6), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 10, 20), actual.getColumn(3));

        assertEquals(2, actual.getRowCount());
        assertEquals(2, innerClosed.get());
        assertEquals(1, outerClosed.get());
    }

    @Test
    void test_left_join_no_populate_with_push_outer_reference_mixed_sizes_multiple_outer_vectors()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2), vv(Type.Any, 4, 5))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), ctx ->
        {
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

        IPhysicalPlan plan = new Sort(0, createIndexLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assumeTrue(plan != null);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());
        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 6, 5), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 1, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 10, 20, null), actual.getColumn(3));

        assertEquals(2, innerClosed.get());
        assertEquals(1, outerClosed.get());
    }

    @Test
    void test_left_join_populate_with_push_outer_reference_mixed_sizes_multiple_outer_vectors()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2), vv(Type.Any, 4, 5))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), ctx ->
        {
            ValueVector col1 = innerClosed.get() == 0 ? vv(Type.Any, 0)
                    : vv(Type.Any, 1, 2);
            ValueVector col2 = innerClosed.get() == 0 ? vv(Type.Any, 10)
                    : vv(Type.Any, 20, 20);
            int rowCount = col1.size();

            return new TupleVector[] { new TupleVector()
            {
                @Override
                public int getRowCount()
                {
                    return rowCount;
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

        IPhysicalPlan plan = createIndexLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a");
        assumeTrue(plan != null);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess, "a"), plan.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 2, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 5, 6), actual.getColumn(1));

        //@formatter:off
        assertVectorsEquals(vv(ResolvedType.table(innerSchema),
                TupleVector.of(innerSchema, List.of(
                    vv(Type.Any, 0),
                    vv(Type.Any, 10)
                )),
                null,
                TupleVector.of(innerSchema, List.of(
                    vv(Type.Any, 1),
                    vv(Type.Any, 20)
                ))
                ), actual.getColumn(2));
        //@formatter:on

        assertEquals(3, actual.getRowCount());
        assertEquals(2, innerClosed.get());
        assertEquals(1, outerClosed.get());
    }

    @Test
    void test_left_join_no_populate_with_push_outer_reference_mixed_sizes_multiple_outer_vectors_no_matches_in_middle_and_last_batch()
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

        IPhysicalPlan plan = new Sort(0, createIndexLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assumeTrue(plan != null);

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        //
        assertVectorsEquals(vv(Type.Any, 0, 1, 2, 100, 200, 1000, 2000), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 6, 5, 400, 500, 4000, 5000), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 1, null, null, null, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 10, 20, null, null, null, null, null), actual.getColumn(3));

        assertEquals(4, innerClosed.get());
        assertEquals(1, outerClosed.get());
    }

    @Test
    void test_left_join_no_populate_with_push_outer_reference_mixed_sizes_multiple_outer_vectors_no_matches_in_middle_and_first_batch()
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

        IPhysicalPlan plan = new Sort(0, createIndexLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assumeTrue(plan != null);
        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        //
        assertVectorsEquals(vv(Type.Any, 0, 1, 2, 100, 200, 1000, 2000), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 6, 5, 400, 500, 4000, 5000), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 1, null, null, null, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 10, 20, null, null, null, null, null), actual.getColumn(3));

        assertEquals(4, innerClosed.get());
        assertEquals(1, outerClosed.get());
    }

    @Test
    void test_left_join_no_populate_with_push_outer_reference_mixed_sizes_multiple_outer_vectors_mixed_matches_in_middle_and_first_batch()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        //@formatter:off
        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 100, 200), vv(Type.Any, 400, 500))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2), vv(Type.Any, 4, 5))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1000, 2000), vv(Type.Any, 4000, 5000))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 6000, 1337), vv(Type.Any, 666, 5555))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 6))));
        //@formatter:on
        IDatasource dsInner = schemaLessDS(() ->
        {
            innerClosed.incrementAndGet();
        }, ctx ->
        {
            int closed = innerClosed.get();

            ValueVector col1 = switch (closed)
            {
                case 0 -> ValueVector.empty(ResolvedType.of(Type.Any));
                case 1 -> vv(Type.Any, 0);
                case 2 -> ValueVector.empty(ResolvedType.of(Type.Any));
                case 3 -> vv(Type.Any, 6000);
                case 4 -> vv(Type.Any, 1, 2);
                default -> null;
            };

            ValueVector col2 = switch (closed)
            {
                case 0 -> ValueVector.empty(ResolvedType.of(Type.Any));
                case 1 -> vv(Type.Any, 10);
                case 2 -> ValueVector.empty(ResolvedType.of(Type.Any));
                case 3 -> vv(Type.Any, "hello world");
                case 4 -> vv(Type.Any, 20, 20);
                default -> null;
            };

            int rowCount = col1.size();

            return new TupleVector[] { new TupleVector()
            {
                @Override
                public int getRowCount()
                {
                    return rowCount;
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

        IPhysicalPlan plan = new Sort(0, createIndexLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assumeTrue(plan != null);
        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        //
        assertVectorsEquals(vv(Type.Any, 0, 1, 2, 100, 200, 1000, 1337, 2000, 6000), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 6, 5, 400, 500, 4000, 5555, 5000, 666), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 1, null, null, null, null, null, null, 6000), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 10, 20, null, null, null, null, null, null, "hello world"), actual.getColumn(3));

        assertEquals(5, innerClosed.get());
        assertEquals(1, outerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_schema_less_smaller_inner()
    {
        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));

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
        assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 1, 2), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_ordinal_columns_smaller_inner()
    {
        Schema outerSchema = Schema.of(Column.of("col1", Type.Int));
        Schema innerSchema = Schema.of(Column.of("col3", Type.Int));
        IExpression col1 = ColumnExpression.Builder.of("col1", ResolvedType.of(Type.Int))
                .withOrdinal(0)
                .build();
        IExpression col3 = ColumnExpression.Builder.of("col3", ResolvedType.of(Type.Int))
                .withOrdinal(1)
                .build();
        ComparisonExpression ce = new ComparisonExpression(IComparisonExpression.Type.EQUAL, col1, col3);

        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Int, 0, 1, 2))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Int, 0, 1))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> ce.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());

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
        assertVectorsEquals(vv(Type.Int, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Int, 0, 1), actual.getColumn(1));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_ordinal_columns()
    {
        Schema outerSchema = Schema.of(Column.of("col1", Type.Int));
        Schema innerSchema = Schema.of(Column.of("col3", Type.Int));
        IExpression col1 = ColumnExpression.Builder.of("col1", ResolvedType.of(Type.Int))
                .withOrdinal(0)
                .build();
        IExpression col3 = ColumnExpression.Builder.of("col3", ResolvedType.of(Type.Int))
                .withOrdinal(1)
                .build();
        ComparisonExpression ce = new ComparisonExpression(IComparisonExpression.Type.EQUAL, col1, col3);

        List<TupleVector> outer = List.of(TupleVector.of(outerSchema, asList(vv(Type.Int, 0, 1))));
        List<TupleVector> inner = List.of(TupleVector.of(innerSchema, asList(vv(Type.Int, 0, 1, 2))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> ce.eval(tv, ctx), null);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());

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
        assertVectorsEquals(vv(Type.Int, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Int, 0, 1), actual.getColumn(1));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    // ---------------------- No populate

    @Test
    void test_inner_join_no_populate_single_vectors_schema_less()
    {
        assertInnerJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    void test_left_join_no_populate_single_vectors_schema_less()
    {
        assertLeftJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    void test_inner_join_no_populate_multiple_outer_vectors_schema_less()
    {
        assertInnerJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    void test_left_join_no_populate_multiple_outer_vectors_schema_less()
    {
        assertLeftJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))), TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    void test_left_join_no_populate_multiple_outer_vectors_schema_less_unkown_estimate_size()
    {
        assertLeftJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))), TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))), true);
    }

    @Test
    void test_inner_join_no_populate_multiple_inner_vectors_schema_less()
    {
        assertInnerJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    void test_left_join_no_populate_multiple_inner_vectors_schema_less()
    {
        assertLeftJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    void test_inner_join_no_populate_multiple_outer_and_inner_vectors_schema_less()
    {
        assertInnerJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    void test_left_join_no_populate_multiple_outer_and_inner_schema_less()
    {
        assertLeftJoinNoPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))), TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    void test_left_join_no_populate_schema_less_no_matching_inner_rows()
    {
        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 4, 5, 6), vv(Type.Any, 1, 2, 3))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    @Test
    void test_left_join_no_populate_no_matching_inner_rows()
    {
        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 4, 5, 6), vv(Type.Any, 1, 2, 3))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    @Test
    void test_left_join_no_populate_schema_less_no_matching_inner_rows_smaller_inner()
    {
        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1, 2), vv(Type.Any, 1, 2, 3))));
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 4, 5), vv(Type.Any, 1, 2))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null, null), actual.getColumn(3));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    @Test
    void test_left_join_no_populate_no_matching_inner_rows_smaller_inner()
    {
        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1, 2), vv(Type.Any, 1, 2, 3))));
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 4, 5), vv(Type.Any, 1, 2))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null, null), actual.getColumn(3));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    @Test
    void test_left_join_no_populate_different_any_data_types_but_equal_when_promote()
    {
        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))), TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6))));
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.Any, "0", "0", "1"), vv(Type.Any, 1, 2, 3))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 0, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 4, 6, 5), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, "0", "0", "1", null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3, null), actual.getColumn(3));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    @Test
    void test_left_join_no_populate_different_types_but_equal_when_promote()
    {
        Schema outerSchema = Schema.of(col("col1", ResolvedType.of(Type.Int), table), col("col2", ResolvedType.of(Type.Any), table));
        Schema innerSchema = Schema.of(col("col3", ResolvedType.of(Type.String), tableB), col("col4", ResolvedType.of(Type.Any), tableB));

        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Int, 0), vv(Type.Any, 4))), TupleVector.of(outerSchema, asList(vv(Type.Int, 2, 1), vv(Type.Any, 5, 6))));
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.String, "0", "0", "1"), vv(Type.Any, 1, 2, 3))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Int, 0, 0, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 4, 6, 5), actual.getColumn(1));
        assertVectorsEquals(vv(Type.String, "0", "0", "1", null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3, null), actual.getColumn(3));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    // ---------------------- Populate

    @Test
    void test_inner_join_populate_single_vectors_schema_less()
    {
        assertInnerJoinWithPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    void test_left_join_populate_single_vectors_schema_less()
    {
        assertLeftJoinPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    void test_inner_join_populate_multiple_outer_vectors_schema_less()
    {
        assertInnerJoinWithPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    void test_left_join_populate_multiple_outer_vectors_schema_less()
    {
        assertLeftJoinPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))), TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3)))));
    }

    @Test
    void test_inner_join_populate_multiple_inner_vectors_schema_less()
    {
        assertInnerJoinWithPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    void test_left_join_populate_multiple_inner_vectors_schema_less()
    {
        assertLeftJoinPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    void test_inner_join_populate_multiple_outer_and_inner_vectors_schema_less()
    {
        assertInnerJoinWithPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    void test_left_join_populate_multiple_outer_and_inner_schema_less()
    {
        assertLeftJoinPopulateSchemaLess(asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))), TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6)))),
                asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 2, 3)))));
    }

    @Test
    void test_left_join_populate_schema_less_no_matching_inner_rows()
    {
        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 4, 5, 6), vv(Type.Any, 1, 2, 3))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess, "a"), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchema), null, null), actual.getColumn(2));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    @Test
    void test_left_join_populate_no_matching_inner_rows()
    {
        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 4, 5, 6), vv(Type.Any, 1, 2, 3))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchema), null, null), actual.getColumn(2));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    @Test
    void test_left_join_populate_schema_less_no_matching_inner_rows_smaller_inner()
    {
        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1, 2), vv(Type.Any, 1, 2, 3))));
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 4, 5), vv(Type.Any, 1, 2))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess, "a"), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchema), null, null, null), actual.getColumn(2));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    @Test
    void test_left_join_populate_no_matching_inner_rows_smaller_inner()
    {
        List<TupleVector> outer = asList(TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1, 2), vv(Type.Any, 1, 2, 3))));
        List<TupleVector> inner = asList(TupleVector.of(innerSchema, asList(vv(Type.Any, 4, 5), vv(Type.Any, 1, 2))));

        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchema), null, null, null), actual.getColumn(2));

        assertTrue(innerClosed.get());
        assertTrue(outerClosed.get());
    }

    // ---------------------- Misc checks

    @Test
    void test_inner_join_no_populate_verify_children_are_closed_with_break()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scan(dsOuter, table, outerSchema, 1), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertTrue(it.hasNext());
        it.close();

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_no_populate_verify_children_are_closed_with_break()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createLeftJoin(scan(dsOuter, table, outerSchema, 1), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertTrue(it.hasNext());
        it.close();

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_populate_verify_children_are_closed_with_break()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = new Limit(0, createInnerJoin(scan(dsOuter, table, outerSchema, 1), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a"), intLit(1));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(1, actual.getRowCount());

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_populate_verify_children_are_closed_with_break()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = new Limit(0, createLeftJoin(scan(dsOuter, table, outerSchema, 1), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a"), intLit(1));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(1, actual.getRowCount());

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_no_outer_vectors()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerOpened = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), new TupleVector[0]);
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 10, 10, 11), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema, () -> innerOpened.set(true)), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());

        if (innerOpened.get())
        {
            assertTrue(innerClosed.get());
        }
        else
        {
            // Inner should not be opened
            assertFalse(innerClosed.get());
        }
    }

    @Test
    void test_inner_join_no_populate_zero_rows_outer_vector()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerOpened = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.EMPTY);
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 10, 10, 11), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema, () -> innerOpened.set(true)), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());

        if (innerOpened.get())
        {
            assertTrue(innerClosed.get());
        }
        else
        {
            // Inner should not be opened
            assertFalse(innerClosed.get());
        }
    }

    @Test
    void test_inner_join_no_populate_no_outer_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerOpened = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.EMPTY);
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 10, 10, 11), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema, () -> innerOpened.set(true)), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        if (innerOpened.get())
        {
            assertTrue(innerClosed.get());
        }
        else
        {
            // Inner should not be opened
            assertFalse(innerClosed.get());
        }
    }

    @Test
    void test_inner_join_populate_no_outer_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerOpened = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), new TupleVector[0]);
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 10, 10, 11), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema, () -> innerOpened.set(true)), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        if (innerOpened.get())
        {
            assertTrue(innerClosed.get());
        }
        else
        {
            // Inner should not be opened
            assertFalse(innerClosed.get());
        }
    }

    @Test
    void test_inner_join_no_populate_no_inner_vectors()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_no_inner_vectors_unknown_estimate()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), true, TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), true, new TupleVector[0]);

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_no_populate_no_inner_vectors_unknown_estimate()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), true, TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), true, new TupleVector[0]);

        IPhysicalPlan plan = new Sort(0, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_no_inner_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.EMPTY);

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());

        try
        {
            it.next();
            fail("Should fail with no such element");
        }
        catch (NoSuchElementException e)
        {

        }

        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_all_null_inner_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, null, null), vv(Type.Any, null, null))));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());

        try
        {
            it.next();
            fail("Should fail with no such element");
        }
        catch (NoSuchElementException e)
        {

        }

        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_all_null_outer_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, null, null), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 4, 5))));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());

        try
        {
            it.next();
            fail("Should fail with no such element");
        }
        catch (NoSuchElementException e)
        {

        }

        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_no_populate_no_inner_rows_schema_less()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertEquals(2, actual.getRowCount());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_no_populate_no_inner_rows_verify_plan_and_runtime_schema_with_asterisk_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = new Sort(0, createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(outerSchema, actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_no_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_outer_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = new Sort(0, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchemaLess), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        assertEquals(outerSchema, actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_no_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_inner_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = new Sort(0, createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchema), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        // Runtime schema is full concat of inner and outer since we have a static inner
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_no_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_outer_and_inner_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = new Sort(0, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());
        assertFalse(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        // Runtime schema is full concat of inner and outer since we have a static inner
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, null, null), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_with_populate_no_inner_rows_verify_plan_and_runtime_schema_with_asterisk_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = new Sort(0, createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess, "a"), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        // Populate always have a concated schema
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchemaLess, "a"), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchemaLess), null, null), actual.getColumn(2));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_with_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_outer_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = new Sort(0, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchemaLess, "a"), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        // Populate always have a concated schema
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchemaLess, "a"), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchemaLess), null, null), actual.getColumn(2));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_with_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_inner_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = new Sort(0, createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchema, "a"), plan.getSchema());
        assertTrue(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        // Populate always have a concated schema
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchema), null, null), actual.getColumn(2));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_left_join_with_populate_no_inner_rows_verify_plan_and_runtime_schema_with_static_outer_and_inner_schema()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = new Sort(0, createLeftJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), plan.getSchema());
        assertFalse(SchemaUtils.isAsterisk(plan.getSchema()));

        TupleVector actual = PlanUtils.concat(context, plan.execute(context));

        // Populate always have a concated schema
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(ResolvedType.table(innerSchema), null, null), actual.getColumn(2));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_populate_no_inner_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), new TupleVector[0]);

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_no_populate_no_matching_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 10, 10, 11), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), null);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    @Test
    void test_inner_join_populate_no_matching_rows()
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaDS(() -> outerClosed.set(true), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.set(true), TupleVector.of(innerSchema, asList(vv(Type.Any, 10, 10, 11), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = createInnerJoin(scanVectors(dsOuter, outerSchema), scanVectors(dsInner, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a");

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();
        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    // @Ignore("Not correct test, there can be a schema when one side is empty. Ie. constant scan")
    // @Test
    // void test_no_schema_when_one_input_is_schema_less()
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
    // plan = createInnerJoin(scanVectors(dsOuter), scanVectors(dsInner), (tv, ctx) -> predicate.eval(tv, ctx), true);
    //
    // assertEquals(Schema.EMPTY, plan.getSchema());
    //
    // dsOuter = schemaDS(() ->
    // {
    // }, TupleVector.of(outerSchema, asList(vv(Type.Object, 0, 1), vv(Type.Object, 1, 2))));
    // dsInner = schemaLessDS(() ->
    // {
    // }, new TupleVector[0]);
    // plan = createInnerJoin(scanVectors(dsOuter), scanVectors(dsInner), (tv, ctx) -> predicate.eval(tv, ctx), true);
    //
    // assertEquals(Schema.EMPTY, plan.getSchema());
    //
    // dsOuter = schemaLessDS(() ->
    // {
    // }, TupleVector.of(outerSchema, asList(vv(Type.Object, 0, 1), vv(Type.Object, 1, 2))));
    // dsInner = schemaLessDS(() ->
    // {
    // }, new TupleVector[0]);
    // plan = createInnerJoin(scanVectors(dsOuter), scanVectors(dsInner), (tv, ctx) -> predicate.eval(tv, ctx), true);
    //
    // assertEquals(Schema.EMPTY, plan.getSchema());
    // }

    private void assertInnerJoinWithPopulateSchemaLess(List<TupleVector> outer, List<TupleVector> inner)
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess, "a"), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), actual.getSchema());
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

        IPhysicalPlan plan = new Sort(2, createInnerJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());
        assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 1, 1, 2), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(3));

        assertTrue(outerClosed.get());
        assertTrue(innerClosed.get());
    }

    private void assertLeftJoinNoPopulateSchemaLess(List<TupleVector> outer, List<TupleVector> inner)
    {
        assertLeftJoinNoPopulateSchemaLess(outer, inner, false);
    }

    private void assertLeftJoinNoPopulateSchemaLess(List<TupleVector> outer, List<TupleVector> inner, boolean unknownEstimateSizes)
    {
        AtomicBoolean outerClosed = new AtomicBoolean();
        AtomicBoolean innerClosed = new AtomicBoolean();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.set(true), unknownEstimateSizes, outer.toArray(new TupleVector[0]));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.set(true), unknownEstimateSizes, inner.toArray(new TupleVector[0]));

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), null),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess), plan.getSchema());

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
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

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

        IPhysicalPlan plan = new Sort(2, createLeftJoin(scanVectors(dsOuter, outerSchemaLess), scanVectors(dsInner, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "a"),
                asList(sortItem(ce("col1"), Order.ASC, NullOrder.UNDEFINED)));

        // Compile time schema
        assertEquals(SchemaUtils.joinSchema(outerSchemaLess, innerSchemaLess, "a"), plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        // Runtime schema
        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema, "a"), actual.getSchema());

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
