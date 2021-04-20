package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.util.ArrayList;
import java.util.List;
import java.util.stream.IntStream;

import org.apache.commons.collections.IteratorUtils;
import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableMeta;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Unit test of {@link GroupByOperator} */
public class GroupByOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("a"), "a")
                .tableMeta(new TableMeta(asList(
                        new TableMeta.Column("col1", DataType.ANY),
                        new TableMeta.Column("col2", DataType.ANY))))
                .build();
        MutableBoolean close = new MutableBoolean();
        Operator op = op(ctx -> IntStream.range(0, 10).mapToObj(i -> (Tuple) Row.of(alias, i, new Object[] {i, i % 2})).iterator(), () -> close.setTrue());

        List<Expression> groupBys = asList(e("a.col2", alias));
        Operator gop = OperatorBuilderUtils.createGroupBy(0, groupBys, new ExpressionIndexValuesFactory(groupBys), op);

        RowIterator it = gop.open(new ExecutionContext(session));

        List<Object> expected = asList(
                asList(0, 2, 4, 6, 8),
                asList(0),
                asList(0),

                asList(1, 3, 5, 7, 9),
                asList(1),
                asList(1));

        List<Object> actual = new ArrayList<>();

        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();

            actual.add(IteratorUtils.toList(IteratorUtils.getIterator(getValue(tuple, 0, "col1"))));
            actual.add(IteratorUtils.toList(IteratorUtils.getIterator(getValue(tuple, 0, "col2"))));
            actual.add(IteratorUtils.toList(IteratorUtils.getIterator(getValue(tuple, 0, "col2"))));
            count++;
        }
        it.close();

        assertEquals(2, count);
        assertEquals(expected, actual);
        assertTrue(close.booleanValue());
    }
}
