package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Collections.emptyList;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.expression.LiteralIntegerExpression;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link IndexSeek} */
public class IndexSeekTest extends APhysicalPlanTest
{
    @Test
    public void test_no_keys()
    {
        TableSourceReference tsf = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("tbl"), "t");
        TupleVector tv1 = TupleVector.of(Schema.of(Column.of("col1", Type.Int)), List.of(VectorTestUtils.vv(Type.Int, 1, 2, 3, 4, 5)));
        IDatasource ds = schemaLessDS(() ->
        {
        }, tv1);
        ISeekPredicate predicate = mock(ISeekPredicate.class);
        when(predicate.getSeekKeys(context)).thenReturn(emptyList());

        IndexSeek is = new IndexSeek(0, Schema.EMPTY, tsf, "", predicate, ds, emptyList());
        assertFalse(is.execute(context)
                .hasNext());
    }

    @Test
    public void test()
    {
        TableSourceReference tsf = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("tbl"), "t");

        TupleVector tv1 = TupleVector.of(Schema.of(Column.of("col1", Type.Int)), List.of(VectorTestUtils.vv(Type.Int, 1, 2, 3, 4, 5)));

        AtomicBoolean closed = new AtomicBoolean();
        IDatasource ds = schemaLessDS(() -> closed.set(true), tv1);
        ISeekPredicate predicate = mock(ISeekPredicate.class);
        when(predicate.getSeekKeys(context)).thenReturn(List.of(() -> VectorTestUtils.vv(Type.Int, 1, 2, 3)));

        // This needs to be redone sometimes, either move SeekPredicate into physicalplan package or make it public, or some
        // other solution :)
        // we are mocking away the SeekPredicate class that caches
        // up seek keys so we fake the cache to verify that the IndexSeek operator clears the cache after execution
        context.getStatementContext()
                .setIndexSeekKeys(0, List.of(() -> VectorTestUtils.vv(Type.Int, 1, 2, 3)));

        IndexSeek is = new IndexSeek(0, Schema.EMPTY, tsf, "", predicate, ds, emptyList());

        assertNotNull(context.getStatementContext()
                .getIndexSeekKeys(0));
        TupleIterator it = is.execute(context);
        assertNull(context.getStatementContext()
                .getIndexSeekKeys(0));

        assertTrue(it.hasNext());

        TupleVector tv1Expected = TupleVector.of(Schema.of(CoreColumn.of("col1", ResolvedType.of(Type.Int), tsf)), List.of(VectorTestUtils.vv(Type.Int, 1, 2, 3, 4, 5)));
        VectorTestUtils.assertTupleVectorsEquals(tv1Expected, it.next());

        assertFalse(it.hasNext());
    }

    @Test
    public void test_batch()
    {
        TableSourceReference tsf = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("tbl"), "t");

        Schema schema = Schema.of(Column.of("col1", Type.Int));
        AtomicInteger closedCount = new AtomicInteger(0);
        IDatasource ds = schemaLessDS(() -> closedCount.incrementAndGet(), ctx ->
        {
            // We should have our keys when executing the data source
            assertNotNull(context.getStatementContext()
                    .getIndexSeekKeys(0));

            // First batch
            if (closedCount.get() == 0)
            {
                return new TupleVector[] { TupleVector.of(schema, vv(Type.Int, 10)), TupleVector.of(schema, vv(Type.Int, 20)) };
            }
            else if (closedCount.get() == 1)
            {
                return new TupleVector[] { TupleVector.of(schema, vv(Type.Int, 30, 40)) };
            }

            return new TupleVector[] { TupleVector.of(schema, vv(Type.Int, 50)) };
        });
        ISeekPredicate predicate = mock(ISeekPredicate.class);
        when(predicate.getSeekKeys(context)).thenReturn(List.of(() -> VectorTestUtils.vv(Type.Int, 1, 2, 3, 4, 5)));

        IndexSeek is = new IndexSeek(0, Schema.EMPTY, tsf, "", predicate, ds, List.of(new Option(IExecutionContext.BATCH_SIZE, new LiteralIntegerExpression(2))));

        TupleIterator it = is.execute(context);
        assertEquals(3, it.estimatedBatchCount());
        assertEquals(5, it.estimatedRowCount());

        Schema expectedSchema = Schema.of(CoreColumn.of("col1", ResolvedType.of(Type.Int), tsf));

        //@formatter:off
        List<TupleVector> expectedVectors = List.of(
                TupleVector.of(expectedSchema, vv(Type.Int, 10, 20)),
                TupleVector.of(expectedSchema, vv(Type.Int, 30, 40)),
                TupleVector.of(expectedSchema, vv(Type.Int, 50))
                );
        //@formatter:on

        int batchCount = 0;
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();

            // Keys should be cleared after each batch
            assertNull(context.getStatementContext()
                    .getIndexSeekKeys(0));

            VectorTestUtils.assertTupleVectorsEquals(expectedVectors.get(batchCount), v);
            rowCount += v.getRowCount();
            batchCount++;
        }

        try
        {
            it.next();
            fail("Should fail");
        }
        catch (NoSuchElementException e)
        {
        }

        it.close();

        assertEquals(3, batchCount);
        assertEquals(5, rowCount);
        assertEquals(3, closedCount.get());
    }

    @Test
    public void test_batch_close_in_middle_of_stream()
    {
        TableSourceReference tsf = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("tbl"), "t");

        Schema schema = Schema.of(Column.of("col1", Type.Int));
        AtomicInteger closedCount = new AtomicInteger(0);
        IDatasource ds = schemaLessDS(() -> closedCount.incrementAndGet(), ctx ->
        {
            // First batch
            if (closedCount.get() == 0)
            {
                return new TupleVector[] { TupleVector.of(schema, vv(Type.Int, 10)), TupleVector.of(schema, vv(Type.Int, 20)) };
            }

            throw new RuntimeException("Should not come here!");
        });
        ISeekPredicate predicate = mock(ISeekPredicate.class);
        when(predicate.getSeekKeys(context)).thenReturn(List.of(() -> VectorTestUtils.vv(Type.Int, 1, 2, 3, 4, 5)));

        IndexSeek is = new IndexSeek(0, Schema.EMPTY, tsf, "", predicate, ds, List.of(new Option(IExecutionContext.BATCH_SIZE, new LiteralIntegerExpression(2))));

        TupleIterator it = is.execute(context);
        assertEquals(3, it.estimatedBatchCount());
        assertEquals(5, it.estimatedRowCount());

        int batchCount = 0;
        int rowCount = 0;
        assertTrue(it.hasNext());
        TupleVector v = it.next();
        rowCount += v.getRowCount();
        batchCount++;
        it.close();

        assertEquals(1, batchCount);
        assertEquals(2, rowCount);
        assertEquals(1, closedCount.get());
    }

    @Test
    public void test_batch_non_asterisk_schema()
    {
        TableSourceReference tsf = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("tbl"), "t");

        Schema schema = Schema.of(CoreColumn.of("col1", ResolvedType.of(Type.Int), tsf));
        AtomicInteger closedCount = new AtomicInteger(0);
        IDatasource ds = schemaLessDS(() -> closedCount.incrementAndGet(), ctx ->
        {
            // First batch
            if (closedCount.get() == 0)
            {
                return new TupleVector[] { TupleVector.of(schema, vv(Type.Int, 10)), TupleVector.of(schema, vv(Type.Int, 20)) };
            }
            else if (closedCount.get() == 1)
            {
                return new TupleVector[] { TupleVector.of(schema, vv(Type.Int, 30, 40)) };
            }

            return new TupleVector[] { TupleVector.of(schema, vv(Type.Int, 50)) };
        });
        ISeekPredicate predicate = mock(ISeekPredicate.class);
        when(predicate.getIndex()).thenReturn(new Index(QualifiedName.of("table"), List.of("col1"), ColumnsType.ALL));
        when(predicate.getSeekKeys(context)).thenReturn(List.of(() -> VectorTestUtils.vv(Type.Int, 1, 2, 3, 4, 5)));

        IndexSeek is = new IndexSeek(0, schema, tsf, "", predicate, ds, List.of(new Option(IExecutionContext.BATCH_SIZE, new LiteralIntegerExpression(2))));

        assertEquals("Index Seek: table [col1] (ALL)", is.getName());

        TupleIterator it = is.execute(context);
        assertEquals(3, it.estimatedBatchCount());
        assertEquals(5, it.estimatedRowCount());

        //@formatter:off
        List<TupleVector> expectedVectors = List.of(
                TupleVector.of(schema, vv(Type.Int, 10, 20)),
                TupleVector.of(schema, vv(Type.Int, 30, 40)),
                TupleVector.of(schema, vv(Type.Int, 50))
                );
        //@formatter:on

        int batchCount = 0;
        int rowCount = 0;
        while (it.hasNext())
        {
            TupleVector v = it.next();
            VectorTestUtils.assertTupleVectorsEquals(expectedVectors.get(batchCount), v);
            rowCount += v.getRowCount();
            batchCount++;
        }

        try
        {
            it.next();
            fail("Should fail");
        }
        catch (NoSuchElementException e)
        {
        }

        it.close();

        assertEquals(3, batchCount);
        assertEquals(5, rowCount);
        assertEquals(3, closedCount.get());
    }
}
