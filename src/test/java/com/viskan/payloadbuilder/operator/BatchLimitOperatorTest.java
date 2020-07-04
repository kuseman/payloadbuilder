package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.catalog.TableAlias;
import com.viskan.payloadbuilder.parser.ExecutionContext;
import com.viskan.payloadbuilder.parser.LiteralExpression;
import com.viskan.payloadbuilder.parser.QualifiedName;

import java.util.Iterator;
import java.util.stream.IntStream;

import org.junit.Test;

/** Unit test of {@link BatchLimitOperator} */
public class BatchLimitOperatorTest extends AOperatorTest
{
    @Test
    public void test()
    {
        TableAlias alias = TableAlias.of(null, QualifiedName.of("a"), "a");
        Operator op = ctx -> IntStream.range(0, 10).mapToObj(i -> Row.of(alias, i, new Object[] {i} )).iterator();                
        Operator limitOp = new BatchLimitOperator(0, op, LiteralExpression.create(5));
        
        ExecutionContext ctx = new ExecutionContext(session);
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
