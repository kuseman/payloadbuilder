package se.kuseman.payloadbuilder.core.utils;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.concurrent.atomic.AtomicInteger;

import org.junit.jupiter.api.Test;

/** Test of {@link WeakListenerList} */
class WeakListenerListTest
{
    @Test
    void test()
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
