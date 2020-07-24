package org.kuse.payloadbuilder.core.operator;

import java.util.Iterator;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.AOperator;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.SortByOperator;
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
        Operator target = new AOperator(0)
        {
            @Override
            public Iterator<Row> open(ExecutionContext context)
            {
                return IntStream.range(0, 100).mapToObj(i -> Row.of(alias, i, new Object[] {rnd.nextInt(100)})).iterator();
            }
        };

        SortByOperator operator = new SortByOperator(
                0,
                target,
                (c, rowA, rowB) -> (int) rowA.getObject(0) - (int) rowB.getObject(0));
        
        Iterator<Row> it = operator.open(new ExecutionContext(session));
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
    }
}
