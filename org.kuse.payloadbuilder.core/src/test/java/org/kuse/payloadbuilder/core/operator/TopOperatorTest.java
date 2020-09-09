package org.kuse.payloadbuilder.core.operator;

import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.LiteralExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test of {@link TopOperator} */
public class TopOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        Random rnd = new Random();
        TableAlias alias = TableAlias.of(null, QualifiedName.of("table"), "a");
        MutableBoolean close = new MutableBoolean();
        Operator target = op(ctx -> IntStream.range(0, 100).mapToObj(i -> Row.of(alias, i, new Object[] {rnd.nextInt(100)})).iterator(), () -> close.setTrue());
        TopOperator operator = new TopOperator(
                0,
                target,
                LiteralExpression.createLiteralNumericExpression("4"));

        RowIterator it = operator.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            it.next();
            count++;
        }
        it.close();
        assertEquals(4, count);
        assertTrue(close.booleanValue());
    }
}
