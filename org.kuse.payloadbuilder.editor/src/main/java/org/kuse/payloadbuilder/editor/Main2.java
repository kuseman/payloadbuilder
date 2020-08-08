package org.kuse.payloadbuilder.editor;

import java.awt.BorderLayout;
import java.awt.Component;
import java.awt.Dimension;
import java.util.ArrayList;
import java.util.List;

import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTable;

public class Main2 extends JFrame
{
    Main2()
    {
        getContentPane().setLayout(new BorderLayout());
        
        JPanel panel = new JPanel();
        panel.setLayout(new BorderLayout());
        getContentPane().add(panel, BorderLayout.CENTER);
        
        List<JTable> tables = new ArrayList<>();
        
        for (int i=0;i<10;i++)
        {
            JTable table = new JTable(new Object[][] {new Object[] { 1,2,3}, new Object[] { 4,5,6 }},  new Object[] { "col1", "col2", "col3" });
            JTable prevTable = tables.size() > 0 ? tables.get(tables.size() - 1) : null;
            if (panel.getComponentCount() == 0)
            {
                panel.add(new JScrollPane(table));
            }
            else if (tables.size() == 1)
            {
                Component c = panel.getComponent(0);
                panel.removeAll();
                JSplitPane splitPane = new JSplitPane();
                splitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
                
                int actualTableHright = (prevTable.getRowCount() + 1) * prevTable.getRowHeight() + 15;
                prevTable.setPreferredSize(new Dimension(0, 40));

                splitPane.setRightComponent(c);
                
                
                splitPane.setLeftComponent(new JScrollPane(table));
                
                panel.add(splitPane);
                
                splitPane.resetToPreferredSizes();
            }
            else
            {
                
            }
            
            tables.add(table);
            
            try
            {
                Thread.sleep(150);
            }
            catch (InterruptedException e)
            {
            }
            
            revalidate();
        }
        
//        JSplitPane
        
        
        
//        JPanel tables = new JPanel();
//        tables.setLayout(new BorderLayout());
//        JButton add = new JButton("add");
//        add.addActionListener(l -> 
//        {
//            int count = tables.getComponentCount();
//            JTable table = new JTable(new Object[][] {new Object[] { 1,2,3}, new Object[] { 4,5,6 }},  new Object[] { "col1", "col2", "col3" });
//            int preferedHeight = 8 * table.getRowHeight();
//            table.setMinimumSize(new Dimension(0, 32));
//            table.setPreferredScrollableViewportSize(new Dimension(0, 32));
//            
//            if (count > 0)
//            {
//                
//                Component c = tables.getComponent(0);
//                JSplitPane splitPane = new JSplitPane();
//                splitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
//                
//                // Switch right component to a new spit pane
//                if (c instanceof JSplitPane)
//                {
//                    JSplitPane sp = (JSplitPane) c;
//                    Component rc = sp.getRightComponent();
//                    
//                    splitPane.setLeftComponent(rc);
//                    splitPane.setRightComponent(new JScrollPane(table));
//                    
//                    sp.setRightComponent(splitPane);
//                }
//                else
//                {
//                    tables.remove(c);
//                    splitPane.setLeftComponent(c);
////                    c.setPreferredSize(new Dimension(0, 80));
//                    splitPane.setRightComponent(new JScrollPane(table));
//                    tables.add(splitPane, BorderLayout.CENTER);
//                }
//                /*
//                 * table1
//                 * 
//                 * split
//                 *   table1
//                 *   table2
//                 *   
//                 * split
//                 *   table1
//                 *   split
//                 *    table2
//                 *    table3
//                 *    
//                 * split
//                 *   table1
//                 *   split
//                 *     table2
//                 *     split
//                 *      table3
//                 *      table4
//                 * 
//                 * 
//                 */
//                
//            }
//            else
//            {
//                tables.add(new JScrollPane(table), BorderLayout.CENTER);
//            }
//            revalidate();
//            
//            System.out.println(table.getHeight());
//        });
//        getContentPane().add(new JScrollPane(tables));
//        getContentPane().add(add);
        
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setSize(new Dimension(800, 600));
        pack();
        setVisible(true);
        
    }
    
    public static void main(String[] args)
    {
        new Main2();
    }
}