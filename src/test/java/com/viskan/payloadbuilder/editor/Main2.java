package com.viskan.payloadbuilder.editor;

import static java.util.Arrays.asList;

import java.awt.Component;
import java.awt.Dimension;
import java.awt.event.ActionEvent;
import java.util.List;

import javax.swing.AbstractAction;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JTable;
import javax.swing.SwingUtilities;
import javax.swing.table.AbstractTableModel;
import javax.swing.table.TableCellRenderer;

import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rsyntaxtextarea.TextEditorPane;
import org.fife.ui.rtextarea.RTextScrollPane;

public class Main2 extends JFrame
{
    Main2()
    {
        
        // 
        
        
//        JTable table = new JTable(new TModel(new String[] {"col", "obj"},  values));
//        table.setRowHeight(30);
//        TableCellRenderer renderer = new ObjectCellRenderer();
//      table.setDefaultRenderer(Object[].class, renderer);
//      table.setDefaultRenderer(List.class, renderer);
        
//        table.addMouseListener(new MouseAdapter()
//        {
//            @Override
//            public void mouseClicked(MouseEvent e)
//            {
//                int col = table.getSelectedColumn();
//                int row = table.getSelectedRow();
//                
//                Object value = table.getValueAt(row, col);
//                
//                if (value instanceof Object[])
//                {
//                    table.getModel().setValueAt(aValue, rowIndex, columnIndex);
//                }
//                
//                super.mouseClicked(e);
//            }
//        });
        TextEditorPane  textEditor = new TextEditorPane();
        textEditor.setReadOnly(true);
        textEditor.setColumns(80);
        textEditor.setRows(40);
        textEditor.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_JSON);
        textEditor.setCodeFoldingEnabled(true);
        
        RTextScrollPane sp = new RTextScrollPane(textEditor);

        
        getContentPane().add(sp);
        setPreferredSize(new Dimension(800, 600));
        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setVisible(true);
        pack();
    }
    
    public static void main(String[] args)
    {
     // Start all Swing applications on the EDT.
        SwingUtilities.invokeLater(() ->
        {
            new Main2();
            //            new PayloadbuilderEditorView().setVisible(true);
        });
    }
    
    public static class ExpandedObject
    {
        Object[] values;
    }
    
    List<Object[]> values = asList(
            new Object[] {1,new Object[] {1,2,3}},
            new Object[] {3,new Object[] {4,5,6}},
            new Object[] {5,new Object[] {7,8,9}},
            new Object[] {7,new Object[] {10,11,12}}
            );
    
    public class ObjectCellRenderer implements TableCellRenderer
    {
        @Override
        public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column)
        {
//            List<Object[]> values = emptyList();
//            if (value instanceof Object[])
//            {
//                values = new ArrayList<>();
//                values.add((Object[]) value);
//            }
//            else if (value instanceof List)
//            {
//                values = (List<Object[]>) value;
//            }
            
            
            JPanel panel = new JPanel();
//            panel.setLayout(new FlowLayout());
            panel.add(new JButton(new AbstractAction("+")
            {
                @Override
                public void actionPerformed(ActionEvent e)
                {
                    System.out.println("Expand " + value);
                }
            }));
            panel.add(new JLabel(String.valueOf(value)));
            return panel;
        }

       
    }
//    
    public static class TModel extends AbstractTableModel
    {
        private final List<Object[]> values;
        private final String[] colnames;
        public TModel(String[] colnames, List<Object[]> values)
        {
            this.colnames = colnames;
            this.values = values;
        }
        
        @Override
        public int getRowCount()
        {
            return values.size();
        }

        @Override
        public int getColumnCount()
        {
            return 2;
        }
        
        @Override
        public String getColumnName(int column)
        {
            return colnames[column];
        }

        @Override
        public Class<?> getColumnClass(int columnIndex)
        {
            switch (columnIndex)
            {
                case 0:
                    return Object.class;
                case 1:
                    return Object[].class;
            }
            return Object.class;
        }
        
        @Override
        public Object getValueAt(int rowIndex, int columnIndex)
        {
            Object val = values.get(rowIndex)[columnIndex];
            
            if (val instanceof Object[])
            {
                return "+" + val;
            }
            
            return val;
        }
       
//        @Override
//        public boolean isCellEditable(int rowIndex, int columnIndex)
//        {
//            return true;
//        }
         
    }
}
