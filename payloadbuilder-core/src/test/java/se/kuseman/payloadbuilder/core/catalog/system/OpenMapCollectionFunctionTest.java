package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.operator.AOperatorTest;

/** Test {@link OpenMapCollectionFunction} */
public class OpenMapCollectionFunctionTest extends AOperatorTest
{
    @Test
    public void test()
    {
        String query = "select * from open_map_collection(@col['attribute1']['buckets'])";
        Operator op = operator(query, emptyMap());

        context.setVariable("col", ofEntries(entry("attribute1",
                ofEntries(entry("buckets", asList(ofEntries(true, entry("key", 10), entry("count", 20)), ofEntries(true, entry("key", 11), entry("count", 15), entry("id", "value"))))))));

        int count = 0;
        TupleIterator it = op.open(context);
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            if (count == 0)
            {
                assertEquals(10, getValue(tuple, 0, "key"));
                assertEquals(20, getValue(tuple, 0, "count"));
                assertNull(getValue(tuple, 0, "id"));
            }
            else
            {
                assertEquals(11, getValue(tuple, 0, "key"));
                assertEquals(15, getValue(tuple, 0, "count"));
                assertEquals("value", getValue(tuple, 0, "id"));
            }
            count++;
        }

        assertEquals(2, count);
    }
}
