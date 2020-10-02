package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Unit test of {@link GroupByOperator} */
public class GroupByOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("a"), "a").columns(new String[] {"col1", "col2"}).build();
        MutableBoolean close = new MutableBoolean();
        Operator op = op(ctx -> IntStream.range(0, 10).mapToObj(i -> (Tuple) Row.of(alias, i, new Object[] {i, i % 2})).iterator(), () -> close.setTrue());

        Operator gop = OperatorBuilderUtils.createGroupBy(0, asList(e("a.col2")), op);

        //        GroupByOperator gop = new GroupByOperator(
        //                0,
        //                op,
        //                asSet(QualifiedName.of("a.col2")),
        //                new ExpressionValuesExtractor(asList(e("a.col2"))),
        ////                (ctx, tuple, values) -> values[0] = tuple.getValue(QualifiedName.of("a", "col2"), 0),
        //                1);

        RowIterator it = gop.open(new ExecutionContext(session));

        List<Object> expected = asList(
                asList(0, 2, 4, 6, 8),
                asList(0),
                asList(0),

                asList(1, 3, 5, 7, 9),
                asList(1),
                asList(1));

        List<Object> actual = new ArrayList<>();

        while (it.hasNext())
        {
            Tuple tuple = it.next();

            actual.add(IteratorUtils.toList(IteratorUtils.getIterator(tuple.getValue(QualifiedName.of("a", "col1"), 0))));
            actual.add(IteratorUtils.toList(IteratorUtils.getIterator(tuple.getValue(QualifiedName.of("col2"), 0))));
            actual.add(IteratorUtils.toList(IteratorUtils.getIterator(tuple.getValue(QualifiedName.of("a", "col2"), 0))));
        }
        it.close();

        assertEquals(expected, actual);
        assertTrue(close.booleanValue());
    }
}
