package se.kuseman.payloadbuilder.core.utils;

import java.util.Iterator;
import java.util.WeakHashMap;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/** Listener list implemented with weak references */
public class WeakListenerList
{
    private static final Logger LOGGER = LoggerFactory.getLogger(WeakListenerList.class);
    private final WeakHashMap<Runnable, Void> map = new WeakHashMap<>();

    /** Register listener */
    public void registerListener(Runnable runnable)
    {
        map.put(runnable, null);
    }

    public void unregisterListener(Runnable runnable)
    {
        map.remove(runnable);
    }

    /** Fire listeners */
    public void fire()
    {
        Iterator<Runnable> iterator = map.keySet()
                .iterator();
        while (iterator.hasNext())
        {
            Runnable next = iterator.next();
            try
            {
                next.run();
            }
            catch (Throwable t)
            {
                LOGGER.warn("Error running listener", t);
            }
        }
    }

}
