package se.kuseman.payloadbuilder.core.cache;

import java.util.Arrays;
import java.util.List;
import java.util.Map;

import org.junit.Assert;
import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Test of {@link SessionBatchCache} */
public class SessionBatchCacheTest extends Assert
{
    @Test
    public void test()
    {
        SessionBatchCache c = new SessionBatchCache();

        Map<Integer, List<Tuple>> map = c.getAll(QualifiedName.of("cache"), Arrays.asList(1, 2, 3));
        assertEquals(3, map.size());
        assertNull(map.get(1));
        assertNull(map.get(2));
        assertNull(map.get(3));
    }
}
