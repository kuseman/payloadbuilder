package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static se.kuseman.payloadbuilder.core.utils.CollectionUtils.asSet;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.assertVectorsEquals;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.Random;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.BiFunction;

import org.assertj.core.api.Assertions;
import org.junit.Ignore;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.expression.AliasExpression;
import se.kuseman.payloadbuilder.core.expression.ArithmeticBinaryExpression;
import se.kuseman.payloadbuilder.core.expression.ColumnExpression;
import se.kuseman.payloadbuilder.core.expression.ComparisonExpression;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link NestedLoop} */
public class NestedLoopTest extends AJoinTest
{
    /**
     * Outer references set that is used to trigger outer nested loop. This isn't actually used in the operator only triggers the function and is used then analyzing operator
     */
    private Set<Column> outerReferences = asSet(col("col", ResolvedType.of(Type.Any), new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "t")));

    @Ignore
    @Test
    public void measure()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        TupleVector tvO = TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2)));
        TupleVector tvI = TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 0, 0, 0, 1, 1, 1, 1, 1), vv(Type.Any, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10)));

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), tvO);
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), tvI);

        // outer.col1 = inner.col1
        IExpression predicate = new ComparisonExpression(IComparisonExpression.Type.EQUAL, ce("col1"), ce("col3"));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), (tv, ctx) -> predicate.eval(tv, ctx), "a", false);

        int ops = 0;
        long time = System.nanoTime();
        for (;;)
        {
            TupleIterator it = plan.execute(context);

            int count = 0;
            int sink = 0;
            while (it.hasNext())
            {
                TupleVector next = it.next();

                assertEquals(3, next.getSchema()
                        .getSize());

                int s = next.getRowCount();

                for (int i = 0; i < s; i++)
                {
                    sink += (Integer) next.getColumn(0)
                            .getAny(i);
                    sink += (Integer) next.getColumn(1)
                            .getAny(i);

                    ValueVector vv = next.getColumn(2);

                    TupleVector tv = vv.getTable(i);

                    int s2 = tv.getRowCount();
                    for (int j = 0; j < s2; j++)
                    {
                        sink += (Integer) tv.getColumn(0)
                                .getAny(j);
                        sink += (Integer) tv.getColumn(1)
                                .getAny(j);
                    }
                }

                // assertVectorsEquals(vv(Type.Object, 0, 0, 1), next.getColumn(0));
                // assertVectorsEquals(vv(Type.Object, 1, 1, 2), next.getColumn(1));
                // assertVectorsEquals(vv(Type.Object, 0, 0, 1), next.getColumn(2));
                // assertVectorsEquals(vv(Type.Object, 1, 2, 3), next.getColumn(3));

                count += s;
            }
            it.close();

            assertEquals(2, count);
            assertTrue(sink > 0);
            // assertEquals(1, outerClosed.get());
            // assertEquals(1, innerClosed.get());

            long seconds = TimeUnit.SECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS);
            ops++;

            // ops/s

            System.out.println(ops / (float) (seconds));

            // System.out.println(TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS));
        }
    }

    // TODO: cross/outer join/apply with populate
    // switched inputs tests

    @Override
    IPhysicalPlan createInnerJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias)
    {
        return NestedLoop.innerJoin(2, outer, inner, predicate, populateAlias, false);
    }

    @Override
    IPhysicalPlan createLeftJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias)
    {
        return NestedLoop.leftJoin(2, outer, inner, predicate, populateAlias, false);
    }

    @Override
    protected IPhysicalPlan createIndexInnerJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias)
    {
        return NestedLoop.innerJoin(2, outer, inner, predicate, populateAlias, true);
    }

    @Override
    protected IPhysicalPlan createIndexLeftJoin(IPhysicalPlan outer, IPhysicalPlan inner, BiFunction<TupleVector, IExecutionContext, ValueVector> predicate, String populateAlias)
    {
        return NestedLoop.leftJoin(2, outer, inner, predicate, populateAlias, true);
    }

    @Test
    public void test_inner_join_constant_scan()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, new ConstantScan(0, TupleVector.CONSTANT), scan(dsInner, tableB, innerSchemaLess), null, false);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(innerSchema, actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 2, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 5, 6), actual.getColumn(1));

        assertEquals(3, actual.getRowCount());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_inner_join_with_populate_no_inner_rows()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger outerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.EMPTY);

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), (tv, ctx) -> predicate.eval(tv, ctx), "p", false);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.EMPTY, actual);

        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_outer_loop_constant_scan_no_inner_rows()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), new TupleVector[0]);

        IPhysicalPlan plan = NestedLoop.leftJoin(2, new ConstantScan(0, TupleVector.CONSTANT), scan(dsInner, tableB, innerSchemaLess), null, false);

        assertEquals(innerSchemaLess, plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(Schema.EMPTY, actual.getSchema());

        assertEquals(1, actual.getRowCount());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_outer_join_constant_scan_no_inner_rows()
    {
        AtomicInteger innerClosed = new AtomicInteger();
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), new TupleVector[0]);

        IPhysicalPlan plan = NestedLoop.leftJoin(2, new ConstantScan(0, TupleVector.CONSTANT), scan(dsInner, tableB, innerSchemaLess), null, false);

        assertEquals(innerSchemaLess, plan.getSchema());

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(Schema.EMPTY, actual.getSchema());

        assertEquals(1, actual.getRowCount());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_outer_loop_no_populate_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger count = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), () ->
        {
            int iter = count.getAndIncrement();
            // Fake no rows for second outer row
            if (iter == 0)
            {
                return new TupleVector[] { TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))) };
            }
            else if (iter == 1)
            {
                return new TupleVector[0];
            }

            return new TupleVector[] { TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))) };
        });

        IPhysicalPlan plan = NestedLoop.leftJoin(2, scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), outerReferences, null, outerSchemaLess);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 0, 1, 2), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 4, 6, 5), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 1, null), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3, null), actual.getColumn(3));

        assertEquals(4, actual.getRowCount());
        assertEquals(1, outerClosed.get());
        // We have 3 outer rows => closed 3 times
        assertEquals(3, innerClosed.get());
    }

    @Test
    public void test_outer_loop_no_populate_no_outer_schema_less_multiple_inner_vectors()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), () -> new TupleVector[] {
                //@formatter:off
                TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))),
                TupleVector.EMPTY,
                TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))) 
                }
                //@formatter:on
        );

        IPhysicalPlan plan = NestedLoop.leftJoin(2, scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess, 1), null, false);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), actual.getSchema());

        assertVectorsEquals(vv(Type.Any, 0, 0, 2, 2, 1, 1, 0, 2, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 4, 5, 5, 6, 6, 4, 5, 6), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 0, 0, 0, 0, 1, 1, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 1, 2, 1, 2, 1, 2, 3, 3, 3), actual.getColumn(3));

        assertEquals(9, actual.getRowCount());
        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_outer_loop_no_outer_vectors_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), new TupleVector[0]);
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), new TupleVector[0]);

        IPhysicalPlan plan = NestedLoop.leftJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchemaLess), outerReferences, null, outerSchema);
        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();

        assertEquals(1, outerClosed.get());
        // Inner should not be opened
        assertEquals(0, innerClosed.get());
    }

    @Test
    public void test_outer_loop_no_outer_rows_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.EMPTY);
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), new TupleVector[0]);

        IPhysicalPlan plan = NestedLoop.leftJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchemaLess), outerReferences, null, outerSchema);
        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();

        assertEquals(1, outerClosed.get());
        // Inner should not be opened
        assertEquals(0, innerClosed.get());
    }

    @Test
    public void test_outer_loop_no_inner_rows_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.EMPTY);

        IPhysicalPlan plan = NestedLoop.leftJoin(2, scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), outerReferences, null, outerSchemaLess);
        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(outerSchema, actual.getSchema());
        assertEquals(3, actual.getRowCount());

        assertVectorsEquals(vv(Type.Any, 0, 2, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 5, 6), actual.getColumn(1));

        assertEquals(1, outerClosed.get());
        assertEquals(3, innerClosed.get());
    }

    @Test
    public void test_outer_loop_multiple_outer_vectors_no_inner_rows_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.EMPTY);

        IPhysicalPlan plan = NestedLoop.leftJoin(2, scan(dsOuter, table, outerSchemaLess, 1), scan(dsInner, tableB, innerSchemaLess), outerReferences, null, outerSchemaLess);
        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(outerSchema, actual.getSchema());
        assertEquals(3, actual.getRowCount());

        assertVectorsEquals(vv(Type.Any, 0, 2, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 5, 6), actual.getColumn(1));

        assertEquals(1, outerClosed.get());
        assertEquals(3, innerClosed.get());
    }

    @Test
    public void test_outer_loop_outer_reference_schema_less_outer_reference_values_are_preserved()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IExpression col1 = new ColumnExpression("col1", "col1", ResolvedType.of(Type.Any), null, 0, true, -1);
        IExpression col2 = new ColumnExpression("col2", "col2", ResolvedType.of(Type.Any), null, 1, true, -1);
        IExpression col3 = new ColumnExpression("col3", "col3", ResolvedType.of(Type.Any), null, 0, false, -1);
        IExpression col4 = new ColumnExpression("col4", "col4", ResolvedType.of(Type.Any), null, 1, false, -1);

        //@formatter:off
        IPhysicalPlan plan = NestedLoop.leftJoin(
                        2,
                        scan(dsOuter, table, outerSchemaLess),
                        new Projection(
                            3,
                            scan(dsInner, tableB, innerSchemaLess),
                            asList(
                              new AliasExpression(col1, "oCol1"),
                              new AliasExpression(col2, "oCol2"),
                              col3,
                              col4,
                              new AliasExpression(new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, col1,
                                      new ArithmeticBinaryExpression(IArithmeticBinaryExpression.Type.ADD, col1, col2)), "oCalc")
                            )),
                        outerReferences,
                        null,
                        outerSchemaLess);
        //@formatter:on

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        //@formatter:off
        Schema expectedSchema = Schema.of(
                col("col1", ResolvedType.of(Type.Any), table),
                col("col2", ResolvedType.of(Type.Any), table),
                col("oCol1", ResolvedType.of(Type.Any), null),
                col("oCol2", ResolvedType.of(Type.Any), null),
                col("col3", ResolvedType.of(Type.Any), null),
                col("col4", ResolvedType.of(Type.Any), null),
                col("oCalc", ResolvedType.of(Type.Any), null)
                );
        //@formatter:on

        Assertions.assertThat(actual.getSchema())
                .usingRecursiveComparison()
                .ignoringFieldsOfTypes(Location.class, Random.class)
                .isEqualTo(expectedSchema);

        //
        assertVectorsEquals(vv(Type.Any, 0, 0, 0, 2, 2, 2, 1, 1, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 4, 4, 5, 5, 5, 6, 6, 6), actual.getColumn(1));
        assertVectorsEquals(vv(Type.Any, 0, 0, 0, 2, 2, 2, 1, 1, 1), actual.getColumn(2));
        assertVectorsEquals(vv(Type.Any, 4, 4, 4, 5, 5, 5, 6, 6, 6), actual.getColumn(3));
        assertVectorsEquals(vv(Type.Any, 0, 0, 1, 0, 0, 1, 0, 0, 1), actual.getColumn(4));
        assertVectorsEquals(vv(Type.Any, 1, 2, 3, 1, 2, 3, 1, 2, 3), actual.getColumn(5));
        assertVectorsEquals(vv(Type.Any, 4, 4, 4, 9, 9, 9, 8, 8, 8), actual.getColumn(6));

        assertEquals(1, outerClosed.get());
        // We have 3 outer rows => closed 3 times
        assertEquals(3, innerClosed.get());
    }

    @Test
    public void test_outer_loop_multiple_outer_vectors_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger count = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), () ->
        {
            int iter = count.getAndIncrement();
            // Fake no rows for second outer row
            if (iter == 0)
            {
                return new TupleVector[] { TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))) };
            }
            else if (iter == 1)
            {
                return new TupleVector[0];
            }

            return new TupleVector[] { TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))) };
        });
        IPhysicalPlan plan = NestedLoop.leftJoin(2, scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), outerReferences, null, outerSchemaLess);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        Schema actualSchema = SchemaUtils.joinSchema(outerSchema, innerSchema);
        assertEquals(actualSchema, actual.getSchema());

        //@formatter:off
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(actualSchema, asList(
                vv(Type.Any, 0, 0, 1, 2),
                vv(Type.Any, 4, 4, 6, 5),
                vv(Type.Any, 0, 0, 1, null),
                vv(Type.Any, 1, 2, 3, null)
                )), actual);
        //@formatter:on

        assertEquals(4, actual.getRowCount());
        assertEquals(1, outerClosed.get());
        // We have 3 outer rows => closed 3 times
        assertEquals(3, innerClosed.get());
    }

    @Test
    public void test_outer_loop_populate_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger count = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), () ->
        {
            int iter = count.getAndIncrement();
            // Fake no rows for second outer row
            if (iter == 0)
            {
                return new TupleVector[] { TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))) };
            }
            else if (iter == 1)
            {
                return new TupleVector[] { TupleVector.EMPTY };
            }

            return new TupleVector[] { TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))) };
        });

        IPhysicalPlan plan = NestedLoop.leftJoin(2, scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), outerReferences, "p", outerSchemaLess);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        //@formatter:off
        Schema actualSchema = Schema.of(
                col("col1", ResolvedType.of(Type.Any), table),
                col("col2", ResolvedType.of(Type.Any), table),
                pop("p", ResolvedType.table(innerSchema), tableB));

        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(actualSchema);

        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(actualSchema, asList(
                vv(Type.Any, 0, 1, 2),
                vv(Type.Any, 4, 6, 5),
                vv(ResolvedType.table(innerSchema),
                     TupleVector.of(innerSchema, asList(
                         vv(Type.Any, 0, 0),
                         vv(Type.Any, 1, 2)
                         )),
                     TupleVector.of(innerSchema, asList(
                         vv(Type.Any, 1),
                         vv(Type.Any, 3)
                         )),
                     null)
                )), actual);
        
        //@formatter:on

        assertEquals(1, outerClosed.get());
        // We have 3 outer rows => closed 3 times
        assertEquals(3, innerClosed.get());
    }

    @Test
    public void test_outer_loop_populate_no_outer_rows_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), new TupleVector[0]);
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), new TupleVector[0]);

        IPhysicalPlan plan = NestedLoop.leftJoin(2, scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), outerReferences, "p", outerSchemaLess);
        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();

        assertEquals(1, outerClosed.get());
        // Inner should not be opened
        assertEquals(0, innerClosed.get());
    }

    @Test
    public void test_outer_loop_populate_no_inner_rows_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 2, 1), vv(Type.Any, 4, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), new TupleVector[0]);

        IPhysicalPlan plan = NestedLoop.leftJoin(2, scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), outerReferences, "p", outerSchemaLess);
        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchemaLess, "p"), actual.getSchema());
        assertEquals(3, actual.getRowCount());

        assertVectorsEquals(vv(Type.Any, 0, 2, 1), actual.getColumn(0));
        assertVectorsEquals(vv(Type.Any, 4, 5, 6), actual.getColumn(1));

        assertEquals(1, outerClosed.get());
        assertEquals(3, innerClosed.get());
    }

    @Test
    public void test_outer_loop_populate_multiple_outer_vectors_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();
        AtomicInteger count = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 4))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 2, 1), vv(Type.Any, 5, 6))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), () ->
        {
            int iter = count.getAndIncrement();
            // Fake no rows for second outer row
            if (iter == 0)
            {
                return new TupleVector[] { TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))) };
            }
            else if (iter == 1)
            {
                return new TupleVector[] { TupleVector.EMPTY };
            }

            return new TupleVector[] { TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))) };
        });

        IPhysicalPlan plan = NestedLoop.leftJoin(2, scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), outerReferences, "p", outerSchemaLess);

        TupleIterator it = plan.execute(context);
        TupleVector actual = PlanUtils.concat(context, it);

        //@formatter:off
        Schema actualSchema = Schema.of(
                col("col1", ResolvedType.of(Type.Any), table),
                col("col2", ResolvedType.of(Type.Any), table),
                pop("p", ResolvedType.table(innerSchema), tableB));

        Assertions.assertThat(actual.getSchema())
            .usingRecursiveComparison()
            .ignoringFieldsOfTypes(Location.class, Random.class)
            .isEqualTo(actualSchema);
        
        VectorTestUtils.assertTupleVectorsEquals(TupleVector.of(actualSchema, asList(
                vv(Type.Any, 0, 1, 2),
                vv(Type.Any, 4, 6, 5),
                vv(ResolvedType.table(innerSchema),
                     TupleVector.of(innerSchema, asList(
                         vv(Type.Any, 0, 0),
                         vv(Type.Any, 1, 2)
                         )),
                     TupleVector.of(innerSchema, asList(
                         vv(Type.Any, 1),
                         vv(Type.Any, 3)
                         )),
                     null)
                )), actual);
        //@formatter:on

        assertEquals(1, outerClosed.get());
        // We have 3 outer rows => closed 3 times
        assertEquals(3, innerClosed.get());
    }

    @Test
    public void test_cross_join_no_populate_no_outer()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), null, false);

        TupleIterator it = plan.execute(context);

        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 0, 1, 1, 1), next.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 1, 1, 2, 2, 2), next.getColumn(1));
            assertVectorsEquals(vv(Type.Any, 0, 0, 1, 0, 0, 1), next.getColumn(2));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3, 1, 2, 3), next.getColumn(3));

            count += next.getRowCount();
        }
        it.close();

        assertEquals(6, count);
        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_cross_join_no_populate_no_outer_switched_inputs()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsInner, tableB, innerSchema), scan(dsOuter, table, outerSchema), null, true);

        TupleIterator it = plan.execute(context);

        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 0, 1, 1, 1), next.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 1, 1, 2, 2, 2), next.getColumn(1));
            assertVectorsEquals(vv(Type.Any, 0, 0, 1, 0, 0, 1), next.getColumn(2));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3, 1, 2, 3), next.getColumn(3));

            count += next.getRowCount();
        }
        it.close();

        assertEquals(6, count);
        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_cross_join_no_populate_verify_children_are_closed_with_break()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema, 1), scan(dsInner, tableB, innerSchema), null, false);

        TupleIterator it = plan.execute(context);

        assertTrue(it.hasNext());
        it.next();
        assertTrue(it.hasNext());
        it.close();

        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    public void test_cross_join_no_populate_no_outer_rows()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), new TupleVector[0]);
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), null, false);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();

        assertEquals(1, outerClosed.get());
        // Inner should not be opened
        assertEquals(0, innerClosed.get());
    }

    @Test
    public void test_cross_join_no_populate_no_inner_rows()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), new TupleVector[0]);

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), null, false);

        assertEquals(SchemaUtils.joinSchema(outerSchema, innerSchema), plan.getSchema());

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();

        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_cross_join_no_populate_multiple_outer_vectors()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema, 1), scan(dsInner, tableB, innerSchema), null, false);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            if (iterations == 0)
            {
                assertVectorsEquals(vv(Type.Any, 0, 0, 0), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 1, 1, 1), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 0, 0, 1), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 1, 2, 3), next.getColumn(3));

            }
            else
            {
                assertVectorsEquals(vv(Type.Any, 1, 1, 1), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 2, 2, 2), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 0, 0, 1), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 1, 2, 3), next.getColumn(3));
            }

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(iterations, 2);
        assertEquals(6, count);
        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    public void test_cross_join_no_populate_multiple_inner_vectors()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))),
                TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema, 2), null, false);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            if (iterations == 0)
            {
                assertVectorsEquals(vv(Type.Any, 0, 0, 1, 1), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 1, 1, 2, 2), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 0, 0, 0, 0), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 1, 2, 1, 2), next.getColumn(3));
            }
            else
            {
                assertVectorsEquals(vv(Type.Any, 0, 1), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 1, 2), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 1, 1), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 3, 3), next.getColumn(3));
            }
            count += next.getRowCount();
            iterations++;
        }
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(iterations, 2);
        assertEquals(6, count);
        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_cross_join_no_populate_multiple_inner_and_outer_vectors()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))),
                TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema, 1), scan(dsInner, tableB, innerSchema, 2), null, false);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            if (iterations == 0)
            {
                assertVectorsEquals(vv(Type.Any, 0, 0), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 1, 1), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 0, 0), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 1, 2), next.getColumn(3));
            }
            else if (iterations == 1)
            {
                assertVectorsEquals(vv(Type.Any, 0), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 1), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 1), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 3), next.getColumn(3));
            }
            else if (iterations == 2)
            {
                assertVectorsEquals(vv(Type.Any, 1, 1), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 2, 2), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 0, 0), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 1, 2), next.getColumn(3));
            }
            else if (iterations == 3)
            {
                assertVectorsEquals(vv(Type.Any, 1), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 2), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 1), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 3), next.getColumn(3));
            }

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(iterations, 4);
        assertEquals(6, count);
        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    public void test_cross_join_no_populate_single_rows()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), null, false);

        TupleIterator it = plan.execute(context);

        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0), next.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1), next.getColumn(1));
            assertVectorsEquals(vv(Type.Any, 0), next.getColumn(2));
            assertVectorsEquals(vv(Type.Any, 1), next.getColumn(3));

            count += next.getRowCount();
        }
        it.close();

        assertEquals(1, count);
        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_inner_loop_no_populate_with_outer()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), outerReferences, null, outerSchema);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 0, 1, 1, 1), next.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 1, 1, 2, 2, 2), next.getColumn(1));
            assertVectorsEquals(vv(Type.Any, 0, 0, 1, 0, 0, 1), next.getColumn(2));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3, 1, 2, 3), next.getColumn(3));

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(iterations, 1);
        assertEquals(6, count);
        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    public void test_inner_loop_populate_with_outer()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), outerReferences, "p", outerSchema);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(3, next.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 1), next.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2), next.getColumn(1));

            ValueVector vv = next.getColumn(2);
            assertEquals(2, vv.size());

            TupleVector actual = vv.getTable(0);
            assertEquals(2, actual.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));

            actual = vv.getTable(1);
            assertEquals(2, actual.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(iterations, 1);
        assertEquals(2, count);
        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    public void test_inner_loop_populate_with_outer_no_inner_rows()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), new TupleVector[0]);

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), outerReferences, "p", outerSchema);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();

        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    public void test_inner_loop_populate_with_outer_no_outer_rows()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), new TupleVector[0]);
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), outerReferences, "p", outerSchema);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();

        assertEquals(1, outerClosed.get());
        // Inner should not been opened
        assertEquals(0, innerClosed.get());
    }

    @Test
    public void test_inner_loop_populate_with_outer_multiple_outer_vectors()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema, 1), scan(dsInner, tableB, innerSchema), outerReferences, "p", outerSchema);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();
            assertEquals(3, next.getSchema()
                    .getSize());

            if (iterations == 0)
            {
                assertVectorsEquals(vv(Type.Any, 0), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 1), next.getColumn(1));
            }
            else
            {
                assertVectorsEquals(vv(Type.Any, 1), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 2), next.getColumn(1));
            }

            ValueVector vv = next.getColumn(2);
            TupleVector actual = vv.getTable(0);
            assertEquals(2, actual.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        assertEquals(2, iterations);
        assertEquals(2, count);
        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    public void test_inner_loop_populate_with_outer_multiple_inner_vectors()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))),
                TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), outerReferences, "p", outerSchema);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(3, next.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 1), next.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2), next.getColumn(1));
            ValueVector vv = next.getColumn(2);
            TupleVector actual = vv.getTable(0);
            assertEquals(2, actual.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        assertEquals(1, iterations);
        assertEquals(2, count);
        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    public void test_cross_join_populate_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), "p", false);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(3, next.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 1), next.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2), next.getColumn(1));

            ValueVector vv = next.getColumn(2);
            assertEquals(2, vv.size());

            TupleVector actual = vv.getTable(0);
            assertEquals(2, actual.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));

            actual = vv.getTable(1);
            assertEquals(2, actual.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(iterations, 1);
        assertEquals(2, count);
        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_cross_join_populate_no_inner_rows_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), new TupleVector[0]);

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), "p", false);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();

        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_cross_join_populate_no_outer_rows_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), new TupleVector[0]);
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), "p", false);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();

        assertEquals(1, outerClosed.get());
        // Inner should not been opened
        assertEquals(0, innerClosed.get());
    }

    @Test
    public void test_cross_join_populate_multiple_outer_vectors_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchemaLess, 1), scan(dsInner, tableB, innerSchemaLess), "p", false);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();
            assertEquals(3, next.getSchema()
                    .getSize());

            if (iterations == 0)
            {
                assertVectorsEquals(vv(Type.Any, 0), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 1), next.getColumn(1));
            }
            else
            {
                assertVectorsEquals(vv(Type.Any, 1), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 2), next.getColumn(1));
            }

            ValueVector vv = next.getColumn(2);
            TupleVector actual = vv.getTable(0);
            assertEquals(2, actual.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        assertEquals(2, iterations);
        assertEquals(2, count);
        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_cross_join_populate_multiple_inner_vectors_schema_less()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaLessDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaLessDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))),
                TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchemaLess), scan(dsInner, tableB, innerSchemaLess), "p", false);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();
            assertEquals(3, next.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 1), next.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2), next.getColumn(1));
            ValueVector vv = next.getColumn(2);
            TupleVector actual = vv.getTable(0);
            assertEquals(2, actual.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 1), actual.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3), actual.getColumn(1));

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        assertEquals(1, iterations);
        assertEquals(2, count);
        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_inner_loop_no_populate_with_outer_single_rows()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), outerReferences, null, outerSchema);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0), next.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1), next.getColumn(1));
            assertVectorsEquals(vv(Type.Any, 0), next.getColumn(2));
            assertVectorsEquals(vv(Type.Any, 1), next.getColumn(3));

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(iterations, 1);
        assertEquals(1, count);
        assertEquals(1, outerClosed.get());
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_inner_loop_no_populate_with_outer_no_outer_rows()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), new TupleVector[0]);
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), outerReferences, null, outerSchema);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(1, outerClosed.get());
        // Inner should not be opened
        assertEquals(0, innerClosed.get());
    }

    @Test
    public void test_inner_loop_no_populate_with_outer_no_inner_rows()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), new TupleVector[0]);

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), outerReferences, null, outerSchema);

        TupleIterator it = plan.execute(context);

        assertFalse(it.hasNext());
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(1, outerClosed.get());
        // Inner should not be opened
        assertEquals(1, innerClosed.get());
    }

    @Test
    public void test_inner_loop_no_populate_with_outer_multiple_outer_vectors()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0, 1), vv(Type.Any, 1, 2, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema, 1), scan(dsInner, tableB, innerSchema), outerReferences, null, outerSchema);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            if (iterations == 0)
            {
                assertVectorsEquals(vv(Type.Any, 0, 0, 0), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 1, 1, 1), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 0, 0, 1), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 1, 2, 3), next.getColumn(3));

            }
            else
            {
                assertVectorsEquals(vv(Type.Any, 1, 1, 1), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 2, 2, 2), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 0, 0, 1), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 1, 2, 3), next.getColumn(3));
            }

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(iterations, 2);
        assertEquals(6, count);
        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    public void test_inner_loop_no_populate_with_outer_multiple_inner_vectors()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0, 1), vv(Type.Any, 1, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))),
                TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(2, scan(dsOuter, table, outerSchema), scan(dsInner, tableB, innerSchema), outerReferences, null, outerSchema);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            assertVectorsEquals(vv(Type.Any, 0, 0, 0, 1, 1, 1), next.getColumn(0));
            assertVectorsEquals(vv(Type.Any, 1, 1, 1, 2, 2, 2), next.getColumn(1));
            assertVectorsEquals(vv(Type.Any, 0, 0, 1, 0, 0, 1), next.getColumn(2));
            assertVectorsEquals(vv(Type.Any, 1, 2, 3, 1, 2, 3), next.getColumn(3));

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(iterations, 1);
        assertEquals(6, count);
        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    public void test_cross_join_no_populate_with_outer_multiple_inner_and_outer_vectors()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))),
                TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))));

        IPhysicalPlan plan = NestedLoop.innerJoin(1, scan(dsOuter, table, outerSchema, 1), scan(dsInner, tableB, innerSchema, 2), outerReferences, null, outerSchema);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            if (iterations == 0)
            {
                assertVectorsEquals(vv(Type.Any, 0, 0, 0), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 1, 1, 1), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 0, 0, 1), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 1, 2, 3), next.getColumn(3));
            }
            else
            {
                assertVectorsEquals(vv(Type.Any, 1, 1, 1), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 2, 2, 2), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 0, 0, 1), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 1, 2, 3), next.getColumn(3));
            }

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(iterations, 2);
        assertEquals(6, count);
        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }

    @Test
    public void test_cross_join_no_populate_with_outer_multiple_inner_and_outer_vectors_2()
    {
        AtomicInteger outerClosed = new AtomicInteger();
        AtomicInteger innerClosed = new AtomicInteger();

        IDatasource dsOuter = schemaDS(() -> outerClosed.incrementAndGet(), TupleVector.of(outerSchema, asList(vv(Type.Any, 0), vv(Type.Any, 1))),
                TupleVector.of(outerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 2))));
        IDatasource dsInner = schemaDS(() -> innerClosed.incrementAndGet(), TupleVector.of(innerSchema, asList(vv(Type.Any, 1), vv(Type.Any, 3))),
                TupleVector.of(innerSchema, asList(vv(Type.Any, 0, 0), vv(Type.Any, 1, 2))));

        IPhysicalPlan plan = NestedLoop.innerJoin(1, scan(dsOuter, table, outerSchema, 1), scan(dsInner, tableB, innerSchema, 1), outerReferences, null, outerSchema);

        TupleIterator it = plan.execute(context);

        int iterations = 0;
        int count = 0;
        while (it.hasNext())
        {
            TupleVector next = it.next();

            assertEquals(4, next.getSchema()
                    .getSize());

            if (iterations == 0)
            {
                assertVectorsEquals(vv(Type.Any, 0, 0, 0), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 1, 1, 1), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 1, 0, 0), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 3, 1, 2), next.getColumn(3));
            }
            else
            {
                assertVectorsEquals(vv(Type.Any, 1, 1, 1), next.getColumn(0));
                assertVectorsEquals(vv(Type.Any, 2, 2, 2), next.getColumn(1));
                assertVectorsEquals(vv(Type.Any, 1, 0, 0), next.getColumn(2));
                assertVectorsEquals(vv(Type.Any, 3, 1, 2), next.getColumn(3));
            }

            count += next.getRowCount();
            iterations++;
        }
        it.close();

        // Even though we are splitting by row we only want one vector back, this to avoid excessive method calling
        assertEquals(iterations, 2);
        assertEquals(6, count);
        assertEquals(1, outerClosed.get());
        assertEquals(2, innerClosed.get());
    }
}
