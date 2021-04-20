package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyMap;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import org.junit.Test;
import org.kuse.payloadbuilder.core.operator.AOperatorTest;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.Tuple;

/** Test {@link OpenMapCollectionFunction} */
public class OpenMapCollectionFunctionTest extends AOperatorTest
{
    @Test
    public void test()
    {
        String query = "select * from open_map_collection(@col['attribute1']['buckets'])";
        Operator op = operator(query, emptyMap());

        context.setVariable("col", ofEntries(entry("attribute1", ofEntries(
                entry("buckets", asList(
                        ofEntries(true, entry("key", 10), entry("count", 20)),
                        ofEntries(true, entry("key", 11), entry("count", 15), entry("id", "value"))))))));

        int count = 0;
        RowIterator it = op.open(context);
        while (it.hasNext())
        {
            Tuple tuple = it.next();
            int pos = (int) getValue(tuple, 0, "__pos");

            if (pos == 0)
            {
                assertEquals(10, getValue(tuple, 0, "key"));
                assertEquals(20, getValue(tuple, 0, "count"));
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
