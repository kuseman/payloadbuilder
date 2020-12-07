package org.kuse.payloadbuilder.core.operator;

import static java.util.Arrays.asList;

import java.util.stream.IntStream;

import org.apache.commons.lang3.mutable.MutableBoolean;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.QualifiedName;

/** Test of {@link UnionOperator} */
public class UnionOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("table"), "a").build();
        MutableBoolean outerClose = new MutableBoolean();
        MutableBoolean innerClose = new MutableBoolean();

        Operator outer = op(ctx -> IntStream.range(0, 10).mapToObj(i -> (Tuple) Row.of(alias, i, new Object[] {i})).iterator(), () -> outerClose.setTrue());
        Operator inner = op(ctx -> IntStream.range(10, 20).mapToObj(i -> (Tuple) Row.of(alias, i, new Object[] {i})).iterator(), () -> innerClose.setTrue());

        UnionOperator operator = new UnionOperator(0, outer, inner, true);

        assertEquals(new UnionOperator(0, outer, inner, true), operator);
        assertEquals(asList(outer, inner), operator.getChildOperators());

        RowIterator it = operator.open(new ExecutionContext(session));
        int count = 0;
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            assertEquals(count, tuple.getValue(QualifiedName.of("__pos"), 0));
            count++;
        }
        it.close();
        assertEquals(20, count);
        assertTrue(outerClose.booleanValue());
        assertTrue(innerClose.booleanValue());
    }
}
