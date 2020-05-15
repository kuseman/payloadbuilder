package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;

import static java.util.Collections.emptyIterator;
import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

/** Test of {@link BatchOperator} */
public class BatchOperatorTest extends Assert
{
    private final TableAlias alias = new TableAlias(null, QualifiedName.of("article"), "a", new String[] {"art_id", "note_id"});
    private final List<BiFunction<EvaluationContext, Row, Object>> indexValueExtractors = new ArrayList<>();

    @Test
    public void test_empty()
    {
        indexValueExtractors.add((ctx, row) -> row.getObject(0));
        BatchOperator op = new BatchOperator(
                alias,
                indexValueExtractors,
                new TestReader(emptyIterator()));
        Iterator<Row> it = op.open(new OperatorContext());
        assertFalse(it.hasNext());
    }

    @Test
    public void test()
    {
        List<Row> outerRows = IntStream.range(0, 5).mapToObj(i -> Row.of(alias, i, new Object[] {i})).collect(toList());
        indexValueExtractors.add((ctx, row) -> row.getObject(0));

        TestReader reader = new TestReader(outerRows.iterator());
        BatchOperator op = new BatchOperator(
                alias,
                indexValueExtractors,
                reader);

        OperatorContext context = new OperatorContext();
        context.setOuterRows(outerRows);
        Iterator<Row> it = op.open(context);
        int count = 0;
        while (it.hasNext())
        {
            it.next();
            assertArrayEquals(new Object[] {count}, reader.actualExtractedValues.get(count));
            count++;
        }
        assertEquals(5, count);
    }

    static class TestReader implements BatchOperator.Reader
    {
        private final List<Object[]> actualExtractedValues = new ArrayList<>();
        private final Iterator<Row> result;

        TestReader(Iterator<Row> result)
        {
            this.result = result;
        }

        @Override
        public Iterator<Row> open(OperatorContext context, TableAlias alias, Iterator<Object[]> indexValuesIterator)
        {
            while (indexValuesIterator.hasNext())
            {
                Object[] values = indexValuesIterator.next();
                actualExtractedValues.add(Arrays.copyOf(values, values.length));
            }
            return result;
        }
    }
}
