package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.QualifiedName;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.collections.IteratorUtils;
import org.junit.Test;

/** Unit test of {@link GroupByOperator} */
public class GroupByOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAlias.of(null, QualifiedName.of("a"), "a");
        Operator op = ctx -> IntStream.range(0, 10).mapToObj(i -> Row.of(alias, i, new Object[] {i, i % 2})).iterator();

        GroupByOperator gop = new GroupByOperator(
                0,
                op,
                emptyList(),
                (ctx, row, values) -> values[0] = row.getObject(1),
                1);

        Iterator<Row> it = gop.open(new ExecutionContext(session));

        List<Object> expected = asList(
                asList(0, 2, 4, 6, 8),
                asList(0, 0, 0, 0, 0),
                asList(1, 3, 5, 7, 9),
                asList(1, 1, 1, 1, 1));

        List<Object> actual = new ArrayList<>();

        while (it.hasNext())
        {
            Row row = it.next();

            actual.add(IteratorUtils.toList(IteratorUtils.getIterator(row.getObject(0))));
            actual.add(IteratorUtils.toList(IteratorUtils.getIterator(row.getObject(1))));
        }

        assertEquals(expected, actual);
    }
}
