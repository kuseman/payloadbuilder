package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;

import static java.util.Collections.emptyList;

import java.util.Iterator;
import java.util.Random;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

/** Test of {@link SortOperator} */
public class SortOperatorTest extends Assert
{
    @Test
    public void test()
    {
        Random rnd = new Random();
        TableAlias alias = TableAlias.of(null, QualifiedName.of("table"), "a");
        Operator target = context -> IntStream.range(0, 100).mapToObj(i -> Row.of(alias, i, new Object[] {rnd.nextInt(100)})).iterator();

        SortOperator operator = new SortOperator(
                target,
                (c, rowA, rowB) -> (int) rowA.getObject(0) - (int) rowB.getObject(0),
                emptyList());
        
        assertEquals(emptyList(), operator.getSortItems());
        
        Iterator<Row> it = operator.open(new OperatorContext());
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
