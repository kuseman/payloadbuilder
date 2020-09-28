package org.kuse.payloadbuilder.editor;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.toList;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.GridBagLayout;
import java.awt.Image;
import java.awt.Toolkit;
import java.awt.event.ActionEvent;
import java.awt.event.InputEvent;
import java.awt.event.KeyEvent;
import java.awt.event.WindowAdapter;
import java.awt.event.WindowEvent;
import java.io.IOException;
import java.util.List;
import java.util.Objects;

import javax.imageio.ImageIO;
import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.Icon;
import javax.swing.InputMap;
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
import org.kuse.payloadbuilder.editor.QueryFileModel.Output;

/** Main view */
class PayloadbuilderEditorView extends JFrame
{
    private static final String TOGGLE_COMMENT = "toggleComment";
    private static final String TOGGLE_RESULT = "toggleResult";
    //    private static final String FORMAT = "Format";
    private static final String NEW_QUERY = "NewQuery";
    private static final String EXECUTE = "Execute";
    private static final String STOP = "Stop";
    private static final String EDIT_VARIABLES = "EditVariables";

    private static final Icon FOLDER_OPEN_O = FontIcon.of(FontAwesome.FOLDER_OPEN_O);
    private static final Icon SAVE = FontIcon.of(FontAwesome.SAVE);
    private static final Icon PLAY_CIRCLE = FontIcon.of(FontAwesome.PLAY_CIRCLE);
    private static final Icon STOP_CIRCLE = FontIcon.of(FontAwesome.STOP_CIRCLE);
    private static final Icon FILE_TEXT_O = FontIcon.of(FontAwesome.FILE_TEXT_O);
    private static final Icon ARROWS_V = FontIcon.of(FontAwesome.ARROWS_V);
    private static final Icon ARROWS_H = FontIcon.of(FontAwesome.ARROWS_H);
    private static final Icon INDENT = FontIcon.of(FontAwesome.INDENT);
    private static final Icon EDIT = FontIcon.of(FontAwesome.EDIT);
    static final List<? extends Image> APPLICATION_ICONS = asList("icons8-database-administrator-48.png", "icons8-database-administrator-96.png")
            .stream()
            .map(name -> PayloadbuilderEditorView.class.getResource("/icons/" + name))
            .map(stream ->
            {
                try
                {
                    return ImageIO.read(stream);
                }
                catch (IOException e)
                {
                    e.printStackTrace();
                    return null;
                }
            })
            .filter(Objects::nonNull)
            .collect(toList());

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
    private Runnable editVariablesRunnable;

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

        KeyStroke executeKeyStroke = KeyStroke.getKeyStroke(KeyEvent.VK_E, InputEvent.CTRL_DOWN_MASK);
        KeyStroke stopKeyStroke = KeyStroke.getKeyStroke(KeyEvent.VK_ESCAPE, 0);
        KeyStroke newQueryKeyStroke = KeyStroke.getKeyStroke(KeyEvent.VK_N, InputEvent.CTRL_DOWN_MASK);
        KeyStroke toggleResultKeyStroke = KeyStroke.getKeyStroke(KeyEvent.VK_R, InputEvent.CTRL_DOWN_MASK);
        KeyStroke toggleCommentKeyStroke = KeyStroke.getKeyStroke(KeyEvent.VK_7, InputEvent.CTRL_DOWN_MASK);

        InputMap inputMap = topPanel.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW);
        inputMap.put(executeKeyStroke, EXECUTE);
        inputMap.put(stopKeyStroke, STOP);
        inputMap.put(newQueryKeyStroke, NEW_QUERY);
        //        topPanel.getInputMap(JComponent.WHEN_IN_FOCUSED_WINDOW).put(KeyStroke.getKeyStroke(KeyEvent.VK_F, InputEvent.CTRL_DOWN_MASK), FORMAT);
        inputMap.put(toggleResultKeyStroke, TOGGLE_RESULT);
        inputMap.put(toggleCommentKeyStroke, TOGGLE_COMMENT);
        topPanel.getActionMap().put(EXECUTE, executeAction);
        topPanel.getActionMap().put(STOP, stopAction);
        topPanel.getActionMap().put(NEW_QUERY, newQueryAction);
        //        topPanel.getActionMap().put(FORMAT, formatAction);
        topPanel.getActionMap().put(TOGGLE_RESULT, toggleResultAction);
        topPanel.getActionMap().put(TOGGLE_COMMENT, toggleCommentAction);

        JButton newQueryButton = new JButton(newQueryAction);
        newQueryButton.setText("New query");
        newQueryButton.setToolTipText("Open new query window (" + getAcceleratorText(newQueryKeyStroke) + ")");

        JButton executeButton = new JButton(executeAction);
        executeButton.setText("Execute");
        executeButton.setToolTipText("Execute query (" + getAcceleratorText(executeKeyStroke) + ")");

        toolBar.add(openAction).setToolTipText("Open file (" + getAcceleratorText(openItem.getAccelerator()) + ")");
        toolBar.add(saveAction).setToolTipText("Save current file (" + getAcceleratorText(saveItem.getAccelerator()) + ")");
        toolBar.addSeparator();
        toolBar.add(newQueryButton);
        toolBar.add(executeButton);
        toolBar.add(stopAction).setToolTipText("Cancel query (" + getAcceleratorText(stopKeyStroke) + ")");
        toolBar.addSeparator();
        toolBar.add(toggleCatalogsAction).setToolTipText("Toggle catalogs pane");
        toolBar.add(toggleResultAction).setToolTipText("Toggle result pane (" + getAcceleratorText(toggleResultKeyStroke) + ")");
        //        toolBar.add(formatAction).setToolTipText("Format query");
        toolBar.add(toggleCommentAction).setToolTipText("Toggle comment on selected lines (" + getAcceleratorText(toggleCommentKeyStroke) + ")");
        toolBar.add(editVariablesAction).setToolTipText("Edit parameters");

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
        panelCatalogs.setLayout(new GridBagLayout());
        splitPane.setLeftComponent(new JScrollPane(panelCatalogs));

        setIconImages(APPLICATION_ICONS);

        addWindowListener(new WindowAdapter()
        {
            @Override
            public void windowClosing(WindowEvent e)
            {
                exitRunnable.run();
            }
        });
        
        setDefaultCloseOperation(DO_NOTHING_ON_CLOSE);
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

    private String getAcceleratorText(KeyStroke accelerator)
    {
        String acceleratorText = "";
        if (accelerator != null)
        {
            int modifiers = accelerator.getModifiers();
            if (modifiers > 0)
            {
                acceleratorText = InputEvent.getModifiersExText(modifiers);
                acceleratorText += "+";
            }
            acceleratorText += KeyEvent.getKeyText(accelerator.getKeyCode());
        }
        return acceleratorText;
    }

    private final Action openAction = new AbstractAction("OPEN", FOLDER_OPEN_O)
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(openRunnable);
        }
    };

    private final Action saveAction = new AbstractAction("SAVE", SAVE)
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

    private final Action executeAction = new AbstractAction(EXECUTE, PLAY_CIRCLE)
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(executeRunnable);
        }
    };

    private final Action stopAction = new AbstractAction(EXECUTE, STOP_CIRCLE)
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(cancelRunnable);
        }
    };

    private final Action newQueryAction = new AbstractAction(NEW_QUERY, FILE_TEXT_O)
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

    private final Action toggleResultAction = new AbstractAction(TOGGLE_RESULT, ARROWS_V)
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(toggleResultRunnable);
        }
    };

    private final Action toggleCatalogsAction = new AbstractAction(TOGGLE_RESULT, ARROWS_H)
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

    private final Action toggleCommentAction = new AbstractAction(TOGGLE_COMMENT, INDENT)
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(toggleCommentRunnable);
        }
    };

    private final Action editVariablesAction = new AbstractAction(EDIT_VARIABLES, EDIT)
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            run(editVariablesRunnable);
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

    void setEditVariablesRunnable(Runnable editVariablesRunnable)
    {
        this.editVariablesRunnable = editVariablesRunnable;
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
