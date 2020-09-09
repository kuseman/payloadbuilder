package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.Date;

import org.junit.Before;
import org.junit.Test;
import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.operator.Row;

/** Test of {@link QualifiedReferenceExpression} */
public class QualifiedReferenceExpressionTest extends AParserTest
{
    private TableAlias a, b, c, d, e;
    private Row aRow;

    @Before
    public void setup()
    {
        /*
         * a
         *   b
         *     c
         *   d
         */
        a = TableAlias.of(null, "tableA", "a");
        a.setColumns(new String[] {"col1", "mapCol"});
        b = TableAlias.of(a, "tableB", "b");
        b.setColumns(new String[] {"col1", "col2"});
        c = TableAlias.of(b, "tableC", "c");
        c.setColumns(new String[] {"col1", "mapCol"});
        d = TableAlias.of(a, "tableD", "d");
        d.setColumns(new String[] {"col1", "mapCol"});
        e = TableAlias.of(a, "tableE", "e");
        e.setColumns(new String[] {"col1"});

        aRow = Row.of(a, 0, new Object[] {1337, ofEntries(entry("key", "value"))});

        for (int i = 0; i < 10; i++)
        {
            Row bRow = Row.of(b, i, new Object[] {"b" + i, new Date()});
            for (int j = 0; j < 10; j++)
            {
                Row cRow = Row.of(c, j, new Object[] {"c" + i, ofEntries(entry("key", "cValue"), entry("key2", ofEntries(entry("subKey", "subValue"))))});
                bRow.getChildRows(0).add(cRow);
                cRow.addParent(bRow);
            }
            aRow.getChildRows(0).add(bRow);
            bRow.addParent(aRow);
        }

        aRow.getChildRows(2).add(Row.of(e, 0, new Object[] {"e0"}));
        aRow.getChildRows(2).get(0).addParent(aRow);
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
        // Child rows
        assertReference(aRow.getChildRows(0), "b");
        // Child rows column (row 0 is chosen)
        assertReference("b0", "b", "col1");
        assertReference("c0", "b", "c", "col1");
        assertReference(null, "d", "col1");
        // Map access
        assertReference("value", "mapCol", "key");
        assertReference(ofEntries(entry("key", "value")), "mapCol");
        assertReference("cValue", "b", "c", "mapCol", "key");
        assertReference("subValue", "b", "c", "mapCol", "key2", "subKey");

        // Parent + child traversal
        // From e TO b
        assertReference("b0", aRow.getChildRows(2).get(0), -1, null, "b", "col1");

        // Parent access with column
        assertReference(1337, aRow.getChildRows(0).get(0).getChildRows(0).get(0), -1, null, "a", "col1");
        // Parent access
        assertReference(asList(aRow), aRow.getChildRows(0).get(0).getChildRows(0).get(0), -1, null, "a");
        // Parent access with child rows
        assertReference(aRow.getChildRows(0), aRow.getChildRows(0).get(0).getChildRows(0).get(0), -1, null, "a", "b");

        //

        // Invalid dereference
        try
        {
            assertReference("value", aRow, -1, new Date(), "b", "col2", "col3");
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
        context.setRow(row);
        assertEquals("Error value for " + e, expected, e.eval(context));
    }
}
