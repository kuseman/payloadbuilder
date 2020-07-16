package com.viskan.payloadbuilder.editor;

import java.awt.AWTEvent;
import java.awt.EventQueue;
import java.io.File;
import java.io.IOException;
import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.HashMap;
import java.util.Map;

import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;

/** Main of PayloadbuilderEditor */
public class Main
{
    private static final File CONFIG_FILE = new File("config.json");
    
    public static void main(String[] args)
    {
        try
        {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | UnsupportedLookAndFeelException ex)
        {
        }
        
        Map<String, Object> config = loadConfig();

        // Start all Swing applications on the EDT.
        SwingUtilities.invokeLater(() ->
        {
//            Toolkit.getDefaultToolkit().getSystemEventQueue().push(new TracingEventQueue());
            
            
            PayloadbuilderEditorView view = new PayloadbuilderEditorView();
            PayloadbuilderEditorModel model = new PayloadbuilderEditorModel(config);
            new PayloadbuilderEditorController(config, view, model);

            view.setVisible(true);
            //            new PayloadbuilderEditorView().setVisible(true);
        });
    }
    
    private static Map<String, Object> loadConfig()
    {
        if (!CONFIG_FILE.exists())
        {
            return new HashMap<>();
        }
        try
        {
            return PayloadbuilderEditorController.MAPPER.readValue(CONFIG_FILE, Map.class);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error loading config from " + CONFIG_FILE.getAbsolutePath(), e);
        }
    }
    
    static void saveConfig(Map<String, Object> config)
    {
        try
        {
            PayloadbuilderEditorController.MAPPER.writeValue(CONFIG_FILE, config);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error saving config to " + CONFIG_FILE.getAbsolutePath(), e);
        }
    }

    public static final class TracingEventQueue extends EventQueue
    {

        //           public static void install() {
        //               trace("Using TracingEventQueue");
        //               Toolkit.getDefaultToolkit().getSystemEventQueue().push(new TracingEventQueue());
        //           }

        private final TracingEventQueueThread tracingThread;

        private TracingEventQueue()
        {
            this.tracingThread = new TracingEventQueueThread(new ThreadGroup("Trace"), 1000);
            this.tracingThread.start();
        }

        @Override
        protected void dispatchEvent(AWTEvent event)
        {
            this.tracingThread.eventDispatched(event);
            super.dispatchEvent(event);
            this.tracingThread.eventProcessed(event);
        }

        private final static class TracingEventQueueThread extends Thread
        {

            private final long thresholdDelay;
            private final ThreadMXBean threadBean;

            private final Map<AWTEvent, Long> eventTimeMap = new HashMap<>(1024);

            public TracingEventQueueThread(final ThreadGroup tg, long thresholdDelay)
            {
                super(tg, "TracingEventQueueThread");
                setDaemon(true);
                this.thresholdDelay = thresholdDelay;
                
                threadBean = ManagementFactory.getThreadMXBean();
            }

            public void eventDispatched(AWTEvent event)
            {
                synchronized (this)
                {
                    this.eventTimeMap.put(event, System.currentTimeMillis());
                }
            }

            public void eventProcessed(AWTEvent event)
            {
                synchronized (this)
                {
                    this.checkEventTime(event, System.currentTimeMillis(),
                            this.eventTimeMap.get(event));
                    this.eventTimeMap.remove(event);
                }
            }

            private void checkEventTime(AWTEvent event, long currTime, long startTime)
            {
                long currProcessingTime = currTime - startTime;
                if (currProcessingTime >= this.thresholdDelay)
                {
                    long threadIds[] = threadBean.getAllThreadIds();
                    for (long threadId : threadIds)
                    {
                        ThreadInfo threadInfo = threadBean.getThreadInfo(threadId, Integer.MAX_VALUE);
                        if (threadInfo.getThreadName().startsWith("AWT-EventQueue"))
                        {
                            System.out.println(threadInfo.getThreadName() + " / " + threadInfo.getThreadState());
                            StackTraceElement[] stack = threadInfo.getStackTrace();
                            for (StackTraceElement stackEntry : stack)
                            {
                                System.out.println("\t" + stackEntry.getClassName() + "." + stackEntry.getMethodName() + " [" + stackEntry.getLineNumber() + "]");
                            }
                        }
                    }

                    System.out.println(("Event [" + event.hashCode() + "] "
                        + event.getClass().getName()
                        + " is taking too much time on EDT (" + currProcessingTime
                        + "): " + event.toString()));
                }
            }

            @Override
            public void run()
            {
                while (true)
                {
                    long currTime = System.currentTimeMillis();
                    synchronized (this)
                    {
                        for (Map.Entry<AWTEvent, Long> entry : this.eventTimeMap.entrySet())
                        {
                            AWTEvent event = entry.getKey();
                            if (entry.getValue() == null)
                            {
                                continue;
                            }
                            long startTime = entry.getValue();
                            this.checkEventTime(event, currTime, startTime);
                        }
                    }
                    try
                    {
                        Thread.sleep(100);
                    }
                    catch (InterruptedException ie)
                    {
                    }
                }
            }
        }
    }

}
