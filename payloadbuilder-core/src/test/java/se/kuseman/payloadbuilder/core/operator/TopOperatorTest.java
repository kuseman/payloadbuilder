package se.kuseman.payloadbuilder.core.operator;

import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableAlias.TableAliasBuilder;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Row;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.parser.LiteralExpression;

/** Test of {@link TopOperator} */
public class TopOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        Random rnd = new Random();
        TableAlias alias = TableAliasBuilder.of(-1, TableAlias.Type.TABLE, QualifiedName.of("table"), "a")
                .build();
        MutableBoolean close = new MutableBoolean();
        Operator target = op(ctx -> IntStream.range(0, 100)
                .mapToObj(i -> (Tuple) Row.of(alias, new String[] { "Value" }, new Object[] { rnd.nextInt(100) }))
                .iterator(), () -> close.setTrue());
        TopOperator operator = new TopOperator(0, target, LiteralExpression.createLiteralNumericExpression("4"));

        TupleIterator it = operator.open(new ExecutionContext(session));
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
