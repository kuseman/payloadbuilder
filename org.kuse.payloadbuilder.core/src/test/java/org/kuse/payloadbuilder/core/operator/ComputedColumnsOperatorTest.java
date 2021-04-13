package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.util.Random;
import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test of {@link ComputedColumnsOperator} */
public class ComputedColumnsOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        Random rnd = new Random();
        TableAlias alias = TableAliasBuilder.of(-1, TableAlias.Type.TABLE, QualifiedName.of("table"), "a").columns(new String[] {"col1"}).build();
        MutableBoolean close = new MutableBoolean();
        Operator target = op(ctx -> IntStream.range(0, 100).mapToObj(i -> (Tuple) Row.of(alias, i, new Object[] {rnd.nextInt(100)})).iterator(), () -> close.setTrue());
        ComputedColumnsOperator operator = new ComputedColumnsOperator(
                0,
                -1,
                target,
                asList("newCol"),
                asList(e("concat('v-', col1)")));

        RowIterator it = operator.open(new ExecutionContext(session));
        while (it.hasNext())
        {
            Tuple tuple = it.next();

            int val = (int) tuple.getValue(tuple.getColmnOrdinal("col1"));
            Object actual = tuple.getValue(tuple.getColmnOrdinal("newCol"));
            assertEquals("v-" + val, actual);
        }
        it.close();
        assertTrue(close.booleanValue());
    }
}
