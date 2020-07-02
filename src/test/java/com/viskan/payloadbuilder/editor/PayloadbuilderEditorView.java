package com.viskan.payloadbuilder.editor;

import com.viskan.payloadbuilder.editor.QueryFileModel.Output;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.BoxLayout;
import javax.swing.JButton;
import javax.swing.JComboBox;
import javax.swing.JComponent;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JMenu;
import javax.swing.JMenuBar;
import javax.swing.JMenuItem;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSeparator;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JToolBar;
import javax.swing.KeyStroke;
import javax.swing.SwingConstants;
import javax.swing.border.EtchedBorder;

import org.kordamp.ikonli.fontawesome.FontAwesome;
import org.kordamp.ikonli.swing.FontIcon;

/** Main view */
class PayloadbuilderEditorView extends JFrame
{
    private static final String TOGGLE_COMMENT = "toggleComment";
    private static final String TOGGLE_RESULT = "toggleResult";
    //    private static final String FORMAT = "Format";
    private static final String NEW_QUERY = "NewQuery";
    private static final String EXECUTE = "Execute";
    private final JSplitPane splitPane;
    private final JTabbedPane tabEditor;
    private final JPanel panelCatalogs;
    private final JPanel panelStatus;
    private final JLabel labelMemory;
    private final JLabel labelCaret;

    private final JMenuItem openItem;
    private final JMenuItem saveItem;
    private final JMenuItem saveAsItem;
    private final JMenuItem exitItem;
    private final JComboBox<QueryFileModel.Output> comboOutput;
    private Runnable executeRunnable;
    private Runnable cancelRunnable;
    private Runnable newQueryRunnable;
    //    private Runnable formatRunnable;
    private Runnable openRunnable;
    private Runnable saveRunnable;
    private Runnable saveAsRunnable;
    private Runnable exitRunnable;
    private Runnable toggleResultRunnable;
    private Runnable toggleCommentRunnable;
    private Runnable outputChangedRunnable;
    
    private boolean catalogsCollapsed = false;
    private int prevCatalogsDividerLocation;
    
    //    private Runnable parametersAction;

    PayloadbuilderEditorView()
    {
        setTitle("Payloadbuilder Editor");
        setLocationRelativeTo(null);
        getContentPane().setLayout(new BorderLayout(0, 0));

        panelStatus = new JPanel();
        panelStatus.setPreferredSize(new Dimension(10, 20));
        getContentPane().add(panelStatus, BorderLayout.SOUTH);
        panelStatus.setLayout(new FlowLayout(FlowLayout.RIGHT, 10, 0));

        labelMemory = new JLabel("", SwingConstants.CENTER);
        labelMemory.setPreferredSize(new Dimension(100, 20));
        labelMemory.setBorder(new EtchedBorder(EtchedBorder.LOWERED));
        labelMemory.setToolTipText("Memory");

        labelCaret = new JLabel("", SwingConstants.CENTER);
        labelCaret.setBorder(new EtchedBorder(EtchedBorder.LOWERED));
        labelCaret.setPreferredSize(new Dimension(100, 20));
        labelCaret.setToolTipText("Caret position (Line, column, position)");

        panelStatus.add(labelMemory);
        panelStatus.add(labelCaret);

        JPanel topPanel = new JPanel();
        topPanel.setLayout(new BorderLayout());
        getContentPane().add(topPanel, BorderLayout.NORTH);

        JMenuBar menuBar = new JMenuBar();
        topPanel.add(menuBar, BorderLayout.NORTH);

        JMenu menu = new JMenu("File");
        openItem = new JMenuItem(openAction);
        openItem.setText("Open");
        openItem.setAccelerator(KeyStroke.getKeyStroke('O', Toolkit.getDefaultToolkit().getMenuShortcutKeyMask()));
        saveItem = new JMenuItem(saveAction);
        saveItem.setText("Save");
        saveItem.setAccelerator(KeyStroke.getKeyStroke('S', Toolkit.getDefaultToolkit().getMenuShortcutKeyMask()));
        saveAsItem = new JMenuItem(saveAsAction);
        saveAsItem.setText("Save As ...");
        exitItem = new JMenuItem(exitAction);
        exitItem.setText("Exit");

        menu.add(openItem);
        menu.add(saveItem);
        menu.add(saveAsItem);
        menu.add(new JSeparator());
        menu.add(exitItem);

        menuBar.add(menu);

        JToolBar toolBar = new JToolBar();
        toolBar.setRollover(true);
        toolBar.setFloatable(false);
        topPanel.add(toolBar, BorderLayout.SOUTH);

        topPanel.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(KeyStroke.getKeyStroke(KeyEvent.VK_E, InputEvent.CTRL_DOWN_MASK), EXECUTE);
        topPanel.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(KeyStroke.getKeyStroke(KeyEvent.VK_N, InputEvent.CTRL_DOWN_MASK), NEW_QUERY);
        //        topPanel.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(KeyStroke.getKeyStroke(KeyEvent.VK_F, InputEvent.CTRL_DOWN_MASK), FORMAT);
        topPanel.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(KeyStroke.getKeyStroke(KeyEvent.VK_R, InputEvent.CTRL_DOWN_MASK), TOGGLE_RESULT);
        topPanel.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(KeyStroke.getKeyStroke(KeyEvent.VK_7, InputEvent.CTRL_DOWN_MASK), TOGGLE_COMMENT);
        topPanel.getActionMap().put(EXECUTE, executeAction);
        topPanel.getActionMap().put(NEW_QUERY, newQueryAction);
        //        topPanel.getActionMap().put(FORMAT, formatAction);
        topPanel.getActionMap().put(TOGGLE_RESULT, toggleResultAction);
        topPanel.getActionMap().put(TOGGLE_COMMENT, toggleCommentAction);

        JButton newQueryButton = new JButton(newQueryAction);
        newQueryButton.setText("New query");
        newQueryButton.setToolTipText("Open new query windows");

        JButton executeButton = new JButton(executeAction);
        executeButton.setText("Execute");
        executeButton.setToolTipText("Execute query");

        toolBar.add(openAction).setToolTipText("Open file");
        toolBar.add(saveAction).setToolTipText("Save current file");
        toolBar.addSeparator();
        toolBar.add(newQueryButton);
        toolBar.add(executeButton);
        toolBar.add(stopAction).setToolTipText("Cancel query");
        toolBar.addSeparator();
        toolBar.add(toggleCatalogsAction).setToolTipText("Toggle catalogs pane");
        toolBar.add(toggleResultAction).setToolTipText("Toggle result pane");
        //        toolBar.add(formatAction).setToolTipText("Format query");
        toolBar.add(toggleCommentAction).setToolTipText("Toggle comment on selected lines");

        comboOutput = new JComboBox<>(Output.values());
        comboOutput.setSelectedItem(Output.TABLE);
        comboOutput.setMaximumSize(new Dimension(150, 20));
        comboOutput.addItemListener(l -> run(outputChangedRunnable));
        toolBar.addSeparator();
        toolBar.add(new JLabel("Output "));
        toolBar.add(comboOutput);

        splitPane = new JSplitPane();
        splitPane.setDividerSize(3);
        getContentPane().add(splitPane, BorderLayout.CENTER);

        tabEditor = new JTabbedPane(SwingConstants.TOP);
        tabEditor.setTabLayoutPolicy(JTabbedPane.SCROLL_TAB_LAYOUT);
        splitPane.setRightComponent(tabEditor);

        panelCatalogs = new JPanel();
        panelCatalogs.setLayout(new BoxLayout(panelCatalogs, BoxLayout.Y_AXIS));
//        panelCatalogs.setPreferredSize(new Dimension(250, 0));
        splitPane.setLeftComponent(new JScrollPane(panelCatalogs));

        setDefaultCloseOperation(EXIT_ON_CLOSE);
        setPreferredSize(new Dimension(1200, 800));
        setLocationByPlatform(true);
        pack();
    }

    //    private void gotoLine()
    //    {
    //        GoToDialog dialog = new GoToDialog(this);
    //        dialog.setMaxLineNumberAllowed(textArea.getLineCount());
    //        dialog.setVisible(true);
    //        int line = dialog.getLineNumber();
    //        if (line>0) {
    //            try {
    //                textArea.setCaretPosition(textArea.getLineStartOffset(line-1));
    //            } catch (BadLocationException ble) { // Never happens
    //                UIManager.getLookAndFeel().provideErrorFeedback(textArea);
    //                ble.printStackTrace();
    //            }
    //        }
    //    }

    private final Action openAction = new AbstractAction("OPEN", (FontIcon.of(FontAwesome.FOLDER_OPEN_O)))
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(openRunnable);
        }
    };

    private final Action saveAction = new AbstractAction("SAVE", (FontIcon.of(FontAwesome.SAVE)))
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(saveRunnable);
        }
    };

    private final Action saveAsAction = new AbstractAction()
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(saveAsRunnable);
        }
    };

    private final Action exitAction = new AbstractAction()
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(exitRunnable);
        }
    };

    private final Action executeAction = new AbstractAction(EXECUTE, FontIcon.of(FontAwesome.PLAY_CIRCLE))
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(executeRunnable);
        }
    };

    private final Action stopAction = new AbstractAction(EXECUTE, FontIcon.of(FontAwesome.STOP_CIRCLE))
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(cancelRunnable);
        }
    };

    private final Action newQueryAction = new AbstractAction(NEW_QUERY, FontIcon.of(FontAwesome.FILE_TEXT_O))
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(newQueryRunnable);
        }
    };

    //    private final Action formatAction = new AbstractAction(FORMAT, FontIcon.of(FontAwesome.REORDER))
    //    {
    //        @Override
    //        public void actionPerformed(ActionEvent e)
    //        {
    //            run(formatRunnable);
    //        }
    //    };

    private final Action toggleResultAction = new AbstractAction(TOGGLE_RESULT, FontIcon.of(FontAwesome.ARROWS_V))
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(toggleResultRunnable);
        }
    };

    private final Action toggleCatalogsAction = new AbstractAction(TOGGLE_RESULT, FontIcon.of(FontAwesome.ARROWS_H))
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
         // Expanded
            if (!catalogsCollapsed)
            {
                prevCatalogsDividerLocation = splitPane.getDividerLocation();
                splitPane.setDividerLocation(0.0d);
                catalogsCollapsed = true;
            }
            else
            {
                splitPane.setDividerLocation(prevCatalogsDividerLocation);
                catalogsCollapsed = false;
            }
        }
    };

    private final Action toggleCommentAction = new AbstractAction(TOGGLE_COMMENT, FontIcon.of(FontAwesome.INDENT))
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(toggleCommentRunnable);
        }
    };

    private void run(Runnable runnable)
    {
        if (runnable != null)
        {
            runnable.run();
        }
    }
    
    JPanel getPanelCatalogs()
    {
        return panelCatalogs;
    }

    JLabel getCaretLabel()
    {
        return labelCaret;
    }

    JLabel getMemoryLabel()
    {
        return labelMemory;
    }
    
    JComboBox<Output> getOutputCombo()
    {
        return comboOutput;
    }

    JTabbedPane getEditorsTabbedPane()
    {
        return tabEditor;
    }

    void setExecuteAction(Runnable executeRunnable)
    {
        this.executeRunnable = executeRunnable;
    }

    void setNewQueryAction(Runnable newQueryRunnable)
    {
        this.newQueryRunnable = newQueryRunnable;
    }

    //    void setFormatAction(Runnable formatRunnable)
    //    {
    //        this.formatRunnable = formatRunnable;
    //    }

    void setOpenAction(Runnable openRunnable)
    {
        this.openRunnable = openRunnable;
    }

    void setSaveAction(Runnable saveRunnable)
    {
        this.saveRunnable = saveRunnable;
    }

    void setSaveAsAction(Runnable saveAsRunnable)
    {
        this.saveAsRunnable = saveAsRunnable;
    }

    void setExitAction(Runnable exitRunnable)
    {
        this.exitRunnable = exitRunnable;
    }

    void setToogleResultAction(Runnable toggleResultRunnable)
    {
        this.toggleResultRunnable = toggleResultRunnable;
    }

    void setToggleCommentRunnable(Runnable toggleCommentRunnable)
    {
        this.toggleCommentRunnable = toggleCommentRunnable;
    }

    void setCancelAction(Runnable cancelRunnable)
    {
        this.cancelRunnable = cancelRunnable;
    }
    
    public void setOutputChangedAction(Runnable outputChangedRunnable)
    {
        this.outputChangedRunnable = outputChangedRunnable;
    }
}
