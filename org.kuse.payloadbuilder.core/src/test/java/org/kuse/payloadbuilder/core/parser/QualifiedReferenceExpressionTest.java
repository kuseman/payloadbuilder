package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.Row;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.TableAlias.TableAliasBuilder;

/** Test of {@link QualifiedReferenceExpression} */
public class QualifiedReferenceExpressionTest extends AParserTest
{
    private TableAlias a;
    private Row aRow;

    @Before
    public void setup()
    {
        a = TableAliasBuilder.of(TableAlias.Type.TABLE, QualifiedName.of("tableA"), "a")
                .columns(new String[] {"col1", "mapCol"})
                .build();

        aRow = Row.of(a, 0, new Object[] {1337, ofEntries(entry("key", "value"))});
    }

    @Test
    public void test_evaluation()
    {
        // No row in context => null
        assertReference(null, null, -1, null, "a");

        // Accessing current row yields null
        assertReference(null, "a");
        // Column ref
        assertReference(1337, "col1");
        assertReference(1337, "a", "col1");
        assertReference(null, "d", "col1");
        // Map access
        assertReference("value", "mapCol", "key");
        assertReference(ofEntries(entry("key", "value")), "mapCol");

        // Invalid dereference
        try
        {
            assertReference("value", aRow, 0, new Date(), "b", "col2", "col3");
            fail();
        }
        catch (IllegalArgumentException e)
        {
            String message = "Cannot dereference value";
            assertTrue("Message should contain " + message + " but was " + e.getMessage(), e.getMessage().contains(message));
        }
    }

    @Test
    public void test_evaluation_lambda()
    {
        // Lambda value in context is null
        assertReference(null, aRow, 0, null, "x", "col1");
        
        // Single lambda value
        assertReference(666, aRow, 0, 666, "x");
        // Row access
        assertReference(1337, aRow, 0, aRow, "x", "col1");
        // Map access
        assertReference("value", aRow, 0, ofEntries(entry("key", "value")), "x", "key");

        // Invalid dereference
        try
        {
            assertReference("value", aRow, 0, new Date(), "x", "key");
            fail();
        }
        catch (IllegalArgumentException e)
        {
            String message = "Cannot dereference value";
            assertTrue("Message should contain " + message + " but was " + e.getMessage(), e.getMessage().contains(message));
        }
    }

    private void assertReference(Object expected, String... parts)
    {
        assertReference(expected, aRow, -1, null, parts);
    }

    private void assertReference(Object expected, Row row, int lambdaId, Object lambdaValue, String... parts)
    {
        QualifiedReferenceExpression e = new QualifiedReferenceExpression(new QualifiedName(asList(parts)), lambdaId);
        if (lambdaId >= 0)
        {
            context.setLambdaValue(lambdaId, lambdaValue);
        }
        context.setTuple(row);
        assertEquals("Error value for " + e, expected, e.eval(context));
    }
}
