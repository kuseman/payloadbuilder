package se.kuseman.payloadbuilder.core.utils;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.Assert;
import org.junit.Test;

/** Test of {@link WeakListenerList} */
public class WeakListenerListTest extends Assert
{
    @Test
    public void test()
    {
        WeakListenerList list = new WeakListenerList();

        AtomicInteger called = new AtomicInteger();
        Runnable r = () ->
        {
            called.incrementAndGet();
        };

        list.registerListener(r);

        list.fire();

        assertEquals(1, called.intValue());

        r = null;
        System.gc();

        list.fire();

        assertEquals(1, called.intValue());
    }
}
