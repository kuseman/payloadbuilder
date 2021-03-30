package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.SortItem.NullOrder;
import org.kuse.payloadbuilder.core.parser.SortItem.Order;

/** Test of {@link SortByOperator} */
public class SortByOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        Random rnd = new Random();
        TableAlias alias = TableAliasBuilder.of(0, TableAlias.Type.TABLE, QualifiedName.of("table"), "a").columns(new String[] {"col1"}).build();
        MutableBoolean close = new MutableBoolean();
        Operator target = op(ctx -> IntStream.range(0, 100).mapToObj(i -> (Tuple) Row.of(alias, i, new Object[] {rnd.nextInt(100)})).iterator(), () -> close.setTrue());
        SortByOperator operator = new SortByOperator(
                0,
                target,
                new ExpressionTupleComparator(asList(new SortItem(e("col1"), Order.ASC, NullOrder.UNDEFINED))));

        RowIterator it = operator.open(new ExecutionContext(session));
        int prev = -1;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int val = (int) tuple.getTuple(0).getValue("col1");
            if (prev != -1)
            {
                assertTrue(prev <= val);
            }

            prev = val;
        }
        it.close();
        assertTrue(close.booleanValue());
    }
}
