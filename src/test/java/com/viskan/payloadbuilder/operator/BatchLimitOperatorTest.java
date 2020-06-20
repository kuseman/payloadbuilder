package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;

import java.util.Iterator;
import java.util.stream.IntStream;

import org.junit.Assert;
import org.junit.Test;

/** Unit test of {@link BatchLimitOperator} */
public class BatchLimitOperatorTest extends Assert
{
    @Test
    public void test()
    {
        TableAlias alias = TableAlias.of(null, QualifiedName.of("a"), "a");
        Operator op = ctx -> IntStream.range(0, 10).mapToObj(i -> Row.of(alias, i, new Object[] {i} )).iterator();                
        Operator limitOp = new BatchLimitOperator(0, op, 5);
        
        OperatorContext ctx = new OperatorContext();
        Iterator<Row> it = limitOp.open(ctx);
        
        int count = 0;
        while (it.hasNext())
        {
            Row row =  it.next();
            assertEquals(count, row.getPos());
            count++;
        }
        
        assertEquals(5, count);

        // Open operator again
        it = limitOp.open(ctx);
        assertTrue(it.hasNext());
        
        while (it.hasNext())
        {
            Row row =  it.next();
            assertEquals(count, row.getPos());
            count++;
        }

        assertEquals(10, count);
        // Open once more
        it = limitOp.open(ctx);
        assertFalse(it.hasNext());
    }
}
