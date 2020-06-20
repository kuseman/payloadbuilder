package com.viskan.payloadbuilder.editor;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;

import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JPanel;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JToolBar;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.border.Border;
import javax.swing.border.LineBorder;

import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rtextarea.RTextScrollPane;

public class PayloadbuilderEditor extends JFrame
{
    private final JPanel panelStatus;
    private final JTabbedPane tabEditor;
    private final JPanel panelResult;
    private final JButton btnExecute;
    private final JPanel panelCatalogs;
    private final JLabel labelMemory;
    
    public PayloadbuilderEditor()
    {
        setTitle("Payloadbuilder Editor");
        setLocationRelativeTo(null);
        getContentPane().setLayout(new BorderLayout(0, 0));
        
        panelStatus = new JPanel();
        panelStatus.setPreferredSize(new Dimension(10, 20));
        getContentPane().add(panelStatus, BorderLayout.SOUTH);
        panelStatus.setLayout(new BoxLayout(panelStatus, BoxLayout.X_AXIS));
        
        labelMemory = new JLabel("Memory");
        labelMemory.setToolTipText("Memory");
        labelMemory.setAlignmentX(Component.RIGHT_ALIGNMENT);
        panelStatus.add(labelMemory);
        
        JPanel topPanel = new JPanel();
        topPanel.setLayout(new BorderLayout());
        getContentPane().add(topPanel, BorderLayout.NORTH);
        
        JMenuBar menuBar = new JMenuBar();
        topPanel.add(menuBar, BorderLayout.NORTH);
        
        menuBar.add(new JMenu("File"));
        
        JToolBar toolBar = new JToolBar();
        toolBar.setFloatable(false);
        topPanel.add(toolBar, BorderLayout.SOUTH);
        
        btnExecute = new JButton("Execute");
        btnExecute.setAlignmentX(Component.CENTER_ALIGNMENT);
        toolBar.add(btnExecute);
        
        JButton btnFormat = new JButton("Format");
        toolBar.add(btnFormat);
        
        JSplitPane splitPane = new JSplitPane();
        splitPane.setDividerSize(2);
        getContentPane().add(splitPane, BorderLayout.CENTER);
        
        JSplitPane splitPaneContent = new JSplitPane();
        splitPaneContent.setAlignmentX(Component.CENTER_ALIGNMENT);
        splitPaneContent.setAlignmentY(Component.CENTER_ALIGNMENT);
        splitPaneContent.setDividerSize(2);
        splitPaneContent.setOrientation(JSplitPane.VERTICAL_SPLIT);
        splitPane.setRightComponent(splitPaneContent);
        
        tabEditor = new JTabbedPane(SwingConstants.TOP);
        splitPaneContent.setLeftComponent(tabEditor);
        
        panelResult = new JPanel();
        panelResult.setAlignmentY(Component.BOTTOM_ALIGNMENT);
        splitPaneContent.setRightComponent(panelResult);
        panelResult.setLayout(new FlowLayout(FlowLayout.CENTER, 5, 5));
        splitPaneContent.setDividerLocation(0);
        splitPaneContent.setDividerLocation(0.9);
        
        panelCatalogs = new JPanel();
        panelCatalogs.setPreferredSize(new Dimension(150, 10));
        splitPane.setLeftComponent(panelCatalogs);

        init();

        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setPreferredSize(new Dimension(1200, 800));
        setLocationByPlatform(true);
        pack();
    }

    private void init()
    {
        RSyntaxTextArea textArea = new RSyntaxTextArea(20, 60);
        textArea.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_SQL);
        textArea.setCodeFoldingEnabled(true);        
        RTextScrollPane sp = new RTextScrollPane(textArea);
        
        tabEditor.add(sp);
        
        JPanel tab = new JPanel();
        tab.setOpaque(false);
        tab.add(new JLabel("File"));
        JLabel close = new JLabel ("X");
        
        close.addMouseListener(new MouseAdapter()
        {
            private final Border BORDER = new LineBorder(Color.BLACK, 1);
            
            @Override
            public void mouseEntered(MouseEvent e)
            {
                close.setBorder(BORDER);
            }
            
            @Override
            public void mouseExited(MouseEvent e)
            {
                close.setBorder(null);
            }
            
            @Override
            public void mouseClicked(MouseEvent e)
            {
                tabEditor.remove(sp);
            }
        });
        tab.add(close);
        
        tabEditor.setTabComponentAt(0, tab);

    }
    
    public static void main(String[] args)
    {
        // Start all Swing applications on the EDT.
        SwingUtilities.invokeLater(() ->
        {
            new PayloadbuilderEditor().setVisible(true);
        });
    }
}
