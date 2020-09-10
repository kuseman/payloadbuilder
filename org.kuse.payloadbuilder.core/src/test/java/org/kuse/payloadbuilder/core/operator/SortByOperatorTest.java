package org.kuse.payloadbuilder.core.operator;

import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test of {@link SortByOperator} */
public class SortByOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        Random rnd = new Random();
        TableAlias alias = TableAlias.of(null, QualifiedName.of("table"), "a");
        MutableBoolean close = new MutableBoolean();
        Operator target = op(ctx -> IntStream.range(0, 100).mapToObj(i -> Row.of(alias, i, new Object[] {rnd.nextInt(100)})).iterator(), () -> close.setTrue());
        SortByOperator operator = new SortByOperator(
                0,
                target,
                (c, rowA, rowB) -> (int) rowA.getObject(0) - (int) rowB.getObject(0));

        RowIterator it = operator.open(new ExecutionContext(session));
        int prev = -1;
        while (it.hasNext())
        {
            Row row = it.next();
            int val = (int) row.getObject(0);
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
