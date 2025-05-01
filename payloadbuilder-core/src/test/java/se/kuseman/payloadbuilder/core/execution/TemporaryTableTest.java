package se.kuseman.payloadbuilder.core.execution;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;
import static se.kuseman.payloadbuilder.test.VectorTestUtils.vv;

import java.util.NoSuchElementException;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.core.physicalplan.APhysicalPlanTest;
import se.kuseman.payloadbuilder.test.VectorTestUtils;

/** Test of {@link TemporaryTable} */
public class TemporaryTableTest extends APhysicalPlanTest
{
    @Test
    public void test_no_indices()
    {
        //@formatter:off
        TupleVector tv = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.String)), asList(
                vv(Type.Int, 1,2,3,1,5,6,7,2),
                vv(Type.String, 100,200,300,400,500,600,700,800)
                ));
        //@formatter:on

        TemporaryTable tempTable = new TemporaryTable(tv, emptyList());
        ISeekPredicate predicate = mock(ISeekPredicate.class);
        when(predicate.getIndex()).thenReturn(new Index(QualifiedName.of("index"), asList("col"), ColumnsType.ANY));
        try
        {
            tempTable.getIndexIterator(context, predicate);
            fail("Should fail because of no temp table indices");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("This tempoarary table does not have any indices"));
        }
    }

    @Test
    public void test_bad_input()
    {
      //@formatter:off
        TupleVector tv = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.String)), asList(
                vv(Type.Int, 1,2,3,1,5,6,7,2),
                vv(Type.String, 100,200,300,400,500,600,700,800)
                ));
        //@formatter:on

        Index index = new Index(QualifiedName.of("temp"), asList("col1"), ColumnsType.ALL);

        TemporaryTable tempTable = new TemporaryTable(tv, asList(index));

        ISeekPredicate predicate = mock(ISeekPredicate.class);
        when(predicate.getIndex()).thenReturn(new Index(QualifiedName.of("index"), asList("col"), ColumnsType.ANY));
        try
        {
            tempTable.getIndexIterator(context, predicate);
            fail("Should fail because of no missing index");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("This tempoarary table does not have index: index [col] (ANY)"));
        }

        try
        {
            index = new Index(QualifiedName.of("temp"), asList("col1"), ColumnsType.ANY);
            tempTable = new TemporaryTable(tv, asList(index));
            fail("Should fail because of wrong index type");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Temporary table indices must have ALL columns type"));
        }

        try
        {
            index = new Index(QualifiedName.of("temp"), asList("col66"), ColumnsType.ALL);
            tempTable = new TemporaryTable(tv, asList(index));
            fail("Should fail because of missing column");
        }
        catch (IllegalArgumentException e)
        {
            assertTrue(e.getMessage(), e.getMessage()
                    .contains("Cannot index temporary table,  missing column: col66 in schema: [col1 (Int), col2 (String)]"));
        }
    }

    @Test
    public void test()
    {
        //@formatter:off
        TupleVector tv = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.String)), asList(
                vv(Type.Int, 1,2,3,1,5,6,7,2),
                vv(Type.String, 100,200,300,400,500,600,700,800)
                ));
        //@formatter:on

        Index index = new Index(QualifiedName.of("temp"), asList("col1"), ColumnsType.ALL);

        TemporaryTable tempTable = new TemporaryTable(tv, asList(index));

        ISeekPredicate predicate = mock(ISeekPredicate.class);
        ISeekPredicate.ISeekKey seekKey = mock(ISeekPredicate.ISeekKey.class);
        when(seekKey.getValue()).thenReturn(vv(Type.Int, 1, 2, 4, 3));
        when(predicate.getIndex()).thenReturn(index);
        when(predicate.getSeekKeys(any())).thenReturn(asList(seekKey));

        TupleIterator iterator = tempTable.getIndexIterator(context, predicate);
        assertEquals(4, iterator.estimatedBatchCount());

        assertTrue(iterator.hasNext());
        TupleVector actual = iterator.next();

        VectorTestUtils.assertTupleVectorsEquals(
        //@formatter:off
                TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.String)), asList(
                        vv(Type.Int, 1, 1),
                        vv(Type.String, 100, 400)
                        ))
        //@formatter:on
                , actual);

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        VectorTestUtils.assertTupleVectorsEquals(
        //@formatter:off
                TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.String)), asList(
                        vv(Type.Int, 2, 2),
                        vv(Type.String, 200, 800)
                        ))
        //@formatter:on
                , actual);

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        VectorTestUtils.assertTupleVectorsEquals(
        //@formatter:off
                TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.String)), asList(
                        vv(Type.Int, 3),
                        vv(Type.String, 300)
                        ))
        //@formatter:on
                , actual);

        assertFalse(iterator.hasNext());
        try
        {
            iterator.next();
            fail("Should fail with NoSuchElement");
        }
        catch (NoSuchElementException e)
        {
        }
    }

    @Test
    public void test_multi_column()
    {
        //@formatter:off
        TupleVector tv = TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.String), Column.of("col3", Type.Float)), asList(
                vv(Type.Int, 1,2,3,1,5,6,7,2),
                vv(Type.String, 100,200,300,400,500,600,700,800),
                vv(Type.Float, 1000.0,1000.0,2000.0,2000.0,3000.0,3000.0,4000.0,4000.0)
                ));
        //@formatter:on

        Index index = new Index(QualifiedName.of("temp"), asList("col1", "col3"), ColumnsType.ALL);

        TemporaryTable tempTable = new TemporaryTable(tv, asList(index));

        ISeekPredicate predicate = mock(ISeekPredicate.class);
        ISeekPredicate.ISeekKey seekKey1 = mock(ISeekPredicate.ISeekKey.class);
        ISeekPredicate.ISeekKey seekKey2 = mock(ISeekPredicate.ISeekKey.class);
        when(seekKey1.getValue()).thenReturn(vv(Type.Int, 1, 2, 4, 3));
        when(seekKey2.getValue()).thenReturn(vv(Type.Float, 1000.0, 2000.0, 8000.0, 2000.0));
        when(predicate.getIndex()).thenReturn(index);
        when(predicate.getSeekKeys(any())).thenReturn(asList(seekKey1, seekKey2));

        TupleIterator iterator = tempTable.getIndexIterator(context, predicate);
        assertEquals(4, iterator.estimatedBatchCount());

        assertTrue(iterator.hasNext());
        TupleVector actual = iterator.next();

        VectorTestUtils.assertTupleVectorsEquals(
        //@formatter:off
                TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.String), Column.of("col3", Type.Float)), asList(
                        vv(Type.Int, 1),
                        vv(Type.String, 100),
                        vv(Type.Float, 1000.0)
                        ))
        //@formatter:on
                , actual);

        assertTrue(iterator.hasNext());
        actual = iterator.next();

        VectorTestUtils.assertTupleVectorsEquals(
        //@formatter:off
                TupleVector.of(Schema.of(Column.of("col1", Type.Int), Column.of("col2", Type.String), Column.of("col3", Type.Float)), asList(
                        vv(Type.Int, 3),
                        vv(Type.String, 300),
                        vv(Type.Float, 2000.0)
                        ))
        //@formatter:on
                , actual);

        assertFalse(iterator.hasNext());
        try
        {
            iterator.next();
            fail("Should fail with NoSuchElement");
        }
        catch (NoSuchElementException e)
        {
        }
    }

    @Test
    public void test_index_scanning_with_different_seek_key_type_than_index_type()
    {
        //@formatter:off
        TupleVector tv = TupleVector.of(Schema.of(Column.of("col1", Type.Int)), asList(
                vv(Type.Int, 1,2,3,1,5,6,7,2)
                ));
        //@formatter:on

        Index index = new Index(QualifiedName.of("temp"), asList("col1"), ColumnsType.ALL);
        TemporaryTable tempTable = new TemporaryTable(tv, asList(index));

        ISeekPredicate predicate = mock(ISeekPredicate.class);
        ISeekPredicate.ISeekKey seekKey1 = mock(ISeekPredicate.ISeekKey.class);
        when(seekKey1.getValue()).thenReturn(vv(Type.String, UTF8String.from("1")));
        when(predicate.getIndex()).thenReturn(index);
        when(predicate.getSeekKeys(any())).thenReturn(asList(seekKey1));

        TupleIterator iterator = tempTable.getIndexIterator(context, predicate);
        assertEquals(1, iterator.estimatedBatchCount());
        assertEquals(1, iterator.estimatedRowCount());

        assertTrue(iterator.hasNext());
        TupleVector actual = iterator.next();

        VectorTestUtils.assertTupleVectorsEquals(
        //@formatter:off
        TupleVector.of(Schema.of(Column.of("col1", Type.Int)), asList(
                vv(Type.Int, 1, 1)
                ))
        , actual);
        //@formatter:on

        assertFalse(iterator.hasNext());
    }

    @Test
    public void test_index_scanning_with_different_seek_key_type_than_index_type_2()
    {
        //@formatter:off
        TupleVector tv = TupleVector.of(Schema.of(Column.of("col1", Type.String)), asList(
                vv(Type.String, "1","2","3","1","5","6","7","2")
                ));
        //@formatter:on

        Index index = new Index(QualifiedName.of("temp"), asList("col1"), ColumnsType.ALL);
        TemporaryTable tempTable = new TemporaryTable(tv, asList(index));

        ISeekPredicate predicate = mock(ISeekPredicate.class);
        ISeekPredicate.ISeekKey seekKey1 = mock(ISeekPredicate.ISeekKey.class);
        when(seekKey1.getValue()).thenReturn(vv(Type.Int, 1));
        when(predicate.getIndex()).thenReturn(index);
        when(predicate.getSeekKeys(any())).thenReturn(asList(seekKey1));

        TupleIterator iterator = tempTable.getIndexIterator(context, predicate);
        assertEquals(1, iterator.estimatedBatchCount());
        assertEquals(1, iterator.estimatedRowCount());

        assertTrue(iterator.hasNext());
        TupleVector actual = iterator.next();

        VectorTestUtils.assertTupleVectorsEquals(
        //@formatter:off
        TupleVector.of(Schema.of(Column.of("col1", Type.String)), asList(
                vv(Type.String, "1", "1")
                ))
        , actual);
        //@formatter:on

        assertFalse(iterator.hasNext());
    }
}
