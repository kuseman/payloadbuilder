package org.kuse.payloadbuilder.editor;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Cursor;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Point;
import java.awt.event.ActionEvent;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.io.IOException;
import java.io.PrintWriter;
import java.io.Writer;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.function.Consumer;

import javax.swing.AbstractAction;
import javax.swing.Action;
import javax.swing.Icon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JPopupMenu;
import javax.swing.JScrollBar;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.JViewport;
import javax.swing.SwingConstants;
import javax.swing.SwingUtilities;
import javax.swing.Timer;
import javax.swing.UIManager;
import javax.swing.WindowConstants;
import javax.swing.event.TableModelEvent;
import javax.swing.table.DefaultTableCellRenderer;
import javax.swing.table.TableColumn;
import javax.swing.text.BadLocationException;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.fife.ui.rsyntaxtextarea.RSyntaxTextArea;
import org.fife.ui.rsyntaxtextarea.SquiggleUnderlineHighlightPainter;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rsyntaxtextarea.TextEditorPane;
import org.fife.ui.rtextarea.RTextScrollPane;
import org.kordamp.ikonli.fontawesome.FontAwesome;
import org.kordamp.ikonli.swing.FontIcon;
import org.kuse.payloadbuilder.editor.QueryFileModel.Output;
import org.kuse.payloadbuilder.editor.QueryFileModel.State;

/** Content of a query editor. Text editor and a result panel separated with a split panel */
class QueryFileView extends JPanel
{
    private static final int COLUMN_ADJUST_ROW_LIMIT = 30;
    private static final Color TABLE_NULL_BACKGROUND = new Color(255, 253, 237);
    private static final Color TABLE_REGULAR_BACKGROUND = UIManager.getColor("Table.dropCellBackground");
    private static final Icon CHECK_CIRCLE_ICON = FontIcon.of(FontAwesome.CHECK_CIRCLE);
    private static final Icon PLAY_ICON = FontIcon.of(FontAwesome.PLAY);
    private static final Icon CLOSE_ICON = FontIcon.of(FontAwesome.CLOSE);
    private static final Icon WARNING_ICON = FontIcon.of(FontAwesome.WARNING);
    private static final Icon STICKY_NOTE_O = FontIcon.of(FontAwesome.STICKY_NOTE_O);
    private static final Icon FILE_CODE_O = FontIcon.of(FontAwesome.FILE_CODE_O);
    private static final int SCROLLBAR_WIDTH = ((Integer) UIManager.get("ScrollBar.width")).intValue();

    private final JSplitPane splitPane;
    private final TextEditorPane textEditor;
    private final JTabbedPane resultTabs;
    private final JTextArea messages;
    private final JPanel resultsPanel;
    private final QueryFileModel file;
    private final JLabel labelRunTime;
    private final JLabel labelRowCount;
    private final JLabel labelExecutionStatus;
    private final JPopupMenu tablePopupMenu = new JPopupMenu();
    private final Timer executionTimer;
    private final PrintWriter messagePrintWriter;
    private final List<ResultTable> tables = new ArrayList<>();
    private final Point tableClickLocation = new Point();

    private boolean resultCollapsed;
    private int prevDividerLocation;

    QueryFileView(
            QueryFileModel file,
            Consumer<String> textChangeAction,
            Consumer<QueryFileView> caretChangeListener)
    {
        this.file = file;
        setLayout(new BorderLayout());

        //CSOFF
        textEditor = new TextEditorPane();
        textEditor.setColumns(80);
        textEditor.setRows(40);
        textEditor.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_SQL);
        textEditor.setCodeFoldingEnabled(true);
        textEditor.setBracketMatchingEnabled(true);
        textEditor.setTabSize(2);
        textEditor.setTabsEmulated(true);
        textEditor.setText(file.getQuery());
        RTextScrollPane sp = new RTextScrollPane(textEditor);
        textEditor.getDocument().addDocumentListener(new ADocumentListenerAdapter()
        {
            @Override
            protected void update()
            {
                textChangeAction.accept(textEditor.getText());
            }
        });
        textEditor.addCaretListener(evt -> caretChangeListener.accept(this));

        messages = new JTextArea();
        resultsPanel = new JPanel();
        resultsPanel.setLayout(new BorderLayout());
        resultTabs = new JTabbedPane();
        resultTabs.add(resultsPanel);
        resultTabs.add(new JScrollPane(messages));
        resultTabs.setTabComponentAt(0, new TabComponentView("Results", FontIcon.of(FontAwesome.TABLE)));
        resultTabs.setTabComponentAt(1, new TabComponentView("Messages", FontIcon.of(FontAwesome.FILE_TEXT_O)));

        splitPane = new JSplitPane();
        splitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
        splitPane.setDividerLocation(400);
        splitPane.setLeftComponent(sp);
        splitPane.setRightComponent(resultTabs);
        add(splitPane, BorderLayout.CENTER);

        JPanel panelStatus = new JPanel();
        panelStatus.setPreferredSize(new Dimension(10, 20));
        add(panelStatus, BorderLayout.SOUTH);
        panelStatus.setLayout(new FlowLayout(FlowLayout.LEFT, 5, 0));
        //CSON
        labelExecutionStatus = new JLabel(getIconFromState(file.getState()));

        labelRunTime = new JLabel("", SwingConstants.LEFT);
        labelRunTime.setToolTipText("Last query run time");
        labelRowCount = new JLabel("", SwingConstants.LEFT);
        labelRowCount.setToolTipText("Last query row count");

        panelStatus.add(labelExecutionStatus);
        panelStatus.add(new JLabel("Time:"));
        panelStatus.add(labelRunTime);
        panelStatus.add(new JLabel("Rows:"));
        panelStatus.add(labelRowCount);

        executionTimer = new Timer(100, l -> setExecutionStats());
        file.addPropertyChangeListener(l ->
        {
            if (QueryFileModel.STATE.equals(l.getPropertyName()))
            {
                handleStateChanged(file, (State) l.getNewValue());
            }
            else if (QueryFileModel.RESULT_MODEL.equals(l.getPropertyName()))
            {
                handleResultModelAdded((ResultModel) l.getNewValue());
            }
        });

        // Redirect query sessions output to messages text area
        messagePrintWriter = getPrintWriter();
        file.getQuerySession().setPrintWriter(messagePrintWriter);
        tablePopupMenu.add(viewAsJsonAction);
        tablePopupMenu.add(viewAsXmlAction);
    }

    private PrintWriter getPrintWriter()
    {
        //CSOFF
        Writer writer = new Writer()
        //CSON
        {
            @Override
            public void write(char[] cbuf, int off, int len) throws IOException
            {
                final String string = new String(cbuf, off, len);
                Runnable r = () -> messages.append(string);
                if (SwingUtilities.isEventDispatchThread())
                {
                    r.run();
                }
                else
                {
                    SwingUtilities.invokeLater(r);
                }
            }

            @Override
            public void flush() throws IOException
            {
            }

            @Override
            public void close() throws IOException
            {
            }
        };

        return new PrintWriter(writer, true)
        {
            @Override
            public void close()
            {
                // DO nothing on close
            }
        };
    }

    private void handleStateChanged(QueryFileModel file, State state)
    {
        labelExecutionStatus.setIcon(getIconFromState(state));
        labelExecutionStatus.setToolTipText(state.getToolTip());

        //CSOFF
        switch (state)
        //CSON
        {
            case EXECUTING:
                resultsPanel.removeAll();
                tables.clear();
                file.clearForExecution();
                messages.setText("");
                executionTimer.start();
                clearHighLights();

                if (file.getOutput() == Output.TABLE)
                {
                    resultTabs.setSelectedIndex(0);
                }
                else
                {
                    resultTabs.setSelectedIndex(1);
                }
                break;
            case COMPLETED:
                setExecutionStats();
                executionTimer.stop();
                resizeLastTablesColumns();
                break;
            case ABORTED:
                setExecutionStats();
                messagePrintWriter.println("Query was aborted!");
                executionTimer.stop();
                break;
            case ERROR:
                resultTabs.setSelectedIndex(1);
                messagePrintWriter.println(file.getError());
                if (file.getParseErrorLocation() != null)
                {
                    highLight(file.getParseErrorLocation().getKey(), file.getParseErrorLocation().getValue());
                }
                break;
        }

        // No rows, then show messages
        if (state != State.EXECUTING && resultsPanel.getComponentCount() == 0)
        {
            resultTabs.setSelectedIndex(1);
        }
    }

    private void resizeLastTablesColumns()
    {
        // Resize last columns if not already done
        if (tables.size() > 0 && !tables.get(tables.size() - 1).columnsAdjusted.get())
        {
            ResultTable lastTable = tables.get(tables.size() - 1);
            lastTable.adjustColumns();
        }
    }

    //CSOFF
    private void handleResultModelAdded(final ResultModel resultModel)
    //CSON
    {
        ResultTable resultTable = createResultTable();
        resultModel.addTableModelListener(e ->
        {
            if (!resultTable.columnsAdjusted.get()
                && e.getType() == TableModelEvent.INSERT
                && resultModel.getRowCount() > COLUMN_ADJUST_ROW_LIMIT)
            {
                resultTable.adjustColumns();
            }
            else if (e.getFirstRow() == TableModelEvent.HEADER_ROW && resultTable.columnsAdjusted.get())
            {
                resultTable.restoreWidths();
            }
        });
        resultTable.setModel(resultModel);

        if (tables.size() > 0)
        {
            resizeLastTablesColumns();
        }
        tables.add(resultTable);
        int size = tables.size();
        resultsPanel.removeAll();

        // 8 rows plus header plus spacing
        //CSOFF
        int tablHeight = resultTable.getRowHeight() * 9 + 10;
        //CSON

        Component parent = null;

        for (int i = 0; i < size; i++)
        {
            JTable table = tables.get(i);
            JTable prevTable = i > 0 ? tables.get(i - 1) : null;
            int prevTableHeight = -1;
            if (i > 0)
            {
                JScrollBar horizontalScrollBar = ((JScrollPane) ((JViewport) prevTable.getParent()).getParent()).getHorizontalScrollBar();

                // The least of 8 rows or actual rows in prev table
                //CSOFF
                prevTableHeight = Math.min((prevTable.getRowCount() + 1) * prevTable.getRowHeight() + 15, tablHeight);
                //CSON
                if (horizontalScrollBar.isVisible())
                {
                    prevTableHeight += SCROLLBAR_WIDTH;
                }
            }
            // Single table
            if (i == 0)
            {
                parent = table;
            }
            // Split panel
            else if (i == 1)
            {
                JSplitPane sp = new JSplitPane();
                sp.setOrientation(JSplitPane.VERTICAL_SPLIT);
                sp.setLeftComponent(new JScrollPane(parent));
                // Adjust prev tables height
                sp.getLeftComponent().setPreferredSize(new Dimension(0, prevTableHeight));
                sp.setRightComponent(new JScrollPane(table));
                sp.getRightComponent().setPreferredSize(new Dimension(0, tablHeight));

                parent = sp;
            }
            // Nested split panel
            else
            {
                JSplitPane prevSp = (JSplitPane) parent;
                Component rc = prevSp.getRightComponent();

                JSplitPane sp = new JSplitPane();
                sp.setOrientation(JSplitPane.VERTICAL_SPLIT);

                // Adjust prev tables height
                if (rc instanceof JScrollPane)
                {
                    sp.setLeftComponent(new JScrollPane(prevTable));
                    sp.getLeftComponent().setPreferredSize(new Dimension(0, prevTableHeight));
                }
                else if (rc instanceof JSplitPane)
                {
                    ((JSplitPane) rc).getRightComponent().setPreferredSize(new Dimension(0, prevTableHeight));
                    sp.setLeftComponent(rc);
                }
                sp.setRightComponent(new JScrollPane(table));
                sp.getRightComponent().setPreferredSize(new Dimension(0, tablHeight));

                JSplitPane topSp = new JSplitPane();
                topSp.setOrientation(JSplitPane.VERTICAL_SPLIT);

                // Replace the right component with the new split panel
                prevSp.setRightComponent(sp);
            }
        }
        resultsPanel.add(new JScrollPane(parent), BorderLayout.CENTER);
    }

    private ResultTable createResultTable()
    {
        ResultTable resultTable = new ResultTable()
        {
            @Override
            public boolean isCellSelected(int row, int column)
            {
                // If cell 0 is selected then select whole row
                if (super.isCellSelected(row, 0))
                {
                    return true;
                }
                return super.isCellSelected(row, column);
            }
        };

        resultTable.addMouseListener(new MouseAdapter()
        {
            @Override
            public void mouseClicked(MouseEvent e)
            {
                tableClickLocation.setLocation(e.getPoint());
            }
        });
        resultTable.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        resultTable.setCellSelectionEnabled(true);
        //CSOFF
        resultTable.setDefaultRenderer(Object.class, new DefaultTableCellRenderer()
        //CSON
        {
            @Override
            public Component getTableCellRendererComponent(JTable table, Object val, boolean isSelected, boolean hasFocus, int row, int column)
            {
                Object value = val;

                if (value != null && value.getClass().isArray())
                {
                    StringBuilder sb = new StringBuilder();
                    int length = Array.getLength(value);
                    sb.append("[");
                    for (int i = 0; i < length; i++)
                    {
                        if (i > 0)
                        {
                            sb.append(", ");
                        }
                        sb.append(Array.get(value, i));
                    }
                    sb.append("]");
                    value = sb.toString();
                }

                super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);

                if (value == null)
                {
                    setText("NULL");
                    if (!isSelected)
                    {
                        setBackground(TABLE_NULL_BACKGROUND);
                    }
                }
                else if (!isSelected)
                {
                    setBackground(TABLE_REGULAR_BACKGROUND);
                }

                return this;
            }
        });
        resultTable.getTableHeader().addMouseListener(new MouseAdapter()
        {
            @Override
            public void mouseClicked(MouseEvent e)
            {
                if (e.getClickCount() == 2 && e.getButton() == MouseEvent.BUTTON1 && resultTable.getTableHeader().getCursor().getType() == Cursor.E_RESIZE_CURSOR)
                {
                    // Move the point a bit to left to avoid resizing wrong column
                    //CSOFF
                    Point p = new Point(e.getPoint().x - 15, e.getPoint().y);
                    //CSON
                    int col = resultTable.getTableHeader().columnAtPoint(p);
                    TableColumn column = resultTable.getColumnModel().getColumn(col);
                    if (column != null)
                    {
                        resultTable.adjuster.adjustColumn(col, -1);
                    }
                }
            }
        });

        //CSOFF
        resultTable.addMouseListener(new MouseAdapter()
        //CSON
        {
            @Override
            public void mouseClicked(MouseEvent e)
            {
                if (e.getClickCount() == 2 && e.getButton() == MouseEvent.BUTTON1)
                {
                    Point point = e.getPoint();
                    int row = resultTable.rowAtPoint(point);
                    int col = resultTable.columnAtPoint(point);

                    if (row >= 0)
                    {
                        showValueDialog(resultTable, resultTable.getValueAt(row, col), row, col, null);
                    }
                }
                else if (e.getClickCount() == 1 && e.getButton() == MouseEvent.BUTTON1)
                {
                    Point point = e.getPoint();
                    int row = resultTable.rowAtPoint(point);
                    int col = resultTable.columnAtPoint(point);
                    if (col == 0)
                    {
                        resultTable.setRowSelectionInterval(row, row);
                    }
                }
            }
        });

        resultTable.setComponentPopupMenu(tablePopupMenu);
        return resultTable;
    }

    //CSOFF
    private final Action viewAsJsonAction = new AbstractAction("View as JSON", STICKY_NOTE_O)
    //CSON
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            JTable table = (JTable) tablePopupMenu.getInvoker();
            int row = table.rowAtPoint(tableClickLocation);
            int col = table.columnAtPoint(tableClickLocation);
            Object value = table.getValueAt(row, col);
            if (value instanceof String)
            {
                try
                {
                    value = ResultModel.READER.readValue((String) value);
                }
                catch (IOException ee)
                {
                }
            }
            showValueDialog(table, value, row, col, SyntaxConstants.SYNTAX_STYLE_JSON);
        }
    };

    //CSOFF
    private final Action viewAsXmlAction = new AbstractAction("View as XML", FILE_CODE_O)
    //CSON
    {
        @Override
        public void actionPerformed(ActionEvent e)
        {
            JTable table = (JTable) tablePopupMenu.getInvoker();
            int row = table.rowAtPoint(tableClickLocation);
            int col = table.columnAtPoint(tableClickLocation);
            Object value = table.getValueAt(row, col);
            if (value instanceof String)
            {
                value = Utils.formatXML((String) value);
            }
            showValueDialog(table, value, row, col, SyntaxConstants.SYNTAX_STYLE_XML);
        }
    };

    private void showValueDialog(JTable resultTable, Object val, int row, int col, String preferredSntax)
    {
        Object value = val;
        if (value == null)
        {
            return;
        }

        if (value.getClass().isArray())
        {
            int length = Array.getLength(value);
            List<Object> list = new ArrayList<>(length);
            for (int i = 0; i < length; i++)
            {
                list.add(Array.get(value, i));
            }
            value = list;
        }

        JFrame frame = new JFrame("Json viewer - " + resultTable.getColumnName(col) + " (Row: " + (row + 1) + ")");
        frame.setIconImages(PayloadbuilderEditorView.APPLICATION_ICONS);
        RSyntaxTextArea rta = new RSyntaxTextArea();
        //CSOFF
        rta.setColumns(80);
        rta.setRows(40);
        //CSON
        if (value instanceof Collection || value instanceof Map)
        {
            // Always use json for map/collection types
            rta.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_JSON);
            rta.setCodeFoldingEnabled(true);
            rta.setBracketMatchingEnabled(true);
            rta.setText(ResultModel.getPrettyJson(value));
        }
        else
        {
            rta.setSyntaxEditingStyle(preferredSntax);
            rta.setText(String.valueOf(value));
        }
        rta.setCaretPosition(0);
        rta.setEditable(false);
        RTextScrollPane sp = new RTextScrollPane(rta);
        frame.getContentPane().add(sp);
        frame.setSize(PayloadbuilderEditorView.DEFAULT_DIALOG_SIZE);
        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
        frame.setVisible(true);
    }

    private Icon getIconFromState(State state)
    {
        //CSOFF
        switch (state)
        //CSOn
        {
            case ABORTED:
                return CLOSE_ICON;
            case EXECUTING:
                return PLAY_ICON;
            case COMPLETED:
                return CHECK_CIRCLE_ICON;
            case ERROR:
                return WARNING_ICON;
        }

        return null;
    }

    private void setExecutionStats()
    {
        labelRunTime.setText(DurationFormatUtils.formatDurationHMS(file.getExecutionTime()));
        labelRowCount.setText(String.valueOf(file.getTotalRowCount()));
        if (file.getResults().size() > 0)
        {
            ResultModel currentModel = file.getResults().get(file.getResults().size() - 1);
            currentModel.notifyChanges();
        }
    }

    int getCaretLineNumber()
    {
        return textEditor.getCaretLineNumber() + 1;
    }

    int getCaretOffsetFromLineStart()
    {
        return textEditor.getCaretOffsetFromLineStart() + 1;
    }

    int getCaretPosition()
    {
        return textEditor.getCaretPosition();
    }

    void toggleResultPane()
    {
        // Expanded
        if (!resultCollapsed)
        {
            prevDividerLocation = splitPane.getDividerLocation();
            splitPane.setDividerLocation(1.0d);
            resultCollapsed = true;
        }
        else
        {
            splitPane.setDividerLocation(prevDividerLocation);
            resultCollapsed = false;
        }
    }

    /**
     * <pre>
     * Toggle comments on selected lines
     * </pre>
     **/
    void toggleComments()
    {
        int lines = textEditor.getLineCount();

        int selStart = textEditor.getSelectionStart();
        int selEnd = textEditor.getSelectionEnd();
        boolean caretSelection = selEnd - selStart == 0;
        Boolean addComments = null;

        try
        {
            List<MutableInt> startOffsets = new ArrayList<>();

            for (int i = 0; i < lines; i++)
            {
                int startOffset = textEditor.getLineStartOffset(i);
                int endOffset = textEditor.getLineEndOffset(i) - 1;

                if (between(startOffset, endOffset, selStart)
                    || (startOffset > selStart && endOffset <= selEnd)
                    || between(startOffset, endOffset, selEnd))
                {
                    if (addComments == null)
                    {
                        addComments = !"--".equals(textEditor.getText(startOffset, 2));
                    }

                    startOffsets.add(new MutableInt(startOffset));
                }
            }

            if (!startOffsets.isEmpty())
            {
                int modifier = 0;
                for (MutableInt startOffset : startOffsets)
                {
                    if (addComments)
                    {
                        textEditor.getDocument().insertString(startOffset.getValue() + modifier, "--", null);
                    }
                    else
                    {
                        textEditor.getDocument().remove(startOffset.getValue() + modifier, 2);
                    }
                    startOffset.setValue(Math.max(startOffset.getValue() + modifier, 0));
                    modifier += addComments ? 2 : -2;
                }

                selStart = startOffsets.get(0).getValue();
                if (!caretSelection)
                {
                    selEnd = startOffsets.get(startOffsets.size() - 1).getValue();
                }
                else
                {
                    selEnd = selStart;
                }
            }

        }
        catch (BadLocationException e)
        {
        }

        textEditor.setSelectionStart(selStart);
        textEditor.setSelectionEnd(selEnd);
    }

    QueryFileModel getFile()
    {
        return file;
    }

    String getQuery(boolean selected)
    {
        return selected && !isBlank(textEditor.getSelectedText()) ? textEditor.getSelectedText() : textEditor.getText();
    }

    void saved()
    {
        textEditor.discardAllEdits();
    }

    private void highLight(int line, int column)
    {
        try
        {
            int pos = Math.max(textEditor.getLineStartOffset(line - 1) + column - 1, 0);
            textEditor.getHighlighter().addHighlight(pos, pos + 3, new SquiggleUnderlineHighlightPainter(Color.RED));
        }
        catch (BadLocationException e)
        {
        }
    }

    private void clearHighLights()
    {
        int selStart = textEditor.getSelectionStart();
        int selEnd = textEditor.getSelectionEnd();

        textEditor.getHighlighter().removeAllHighlights();

        textEditor.setSelectionStart(selStart);
        textEditor.setSelectionEnd(selEnd);
    }

    @Override
    public boolean requestFocusInWindow()
    {
        textEditor.requestFocusInWindow();
        return true;
    }

    private boolean between(int start, int end, int value)
    {
        return value >= start && value <= end;
    }

    /** Wrapper class with a connected column adjuster */
    private static class ResultTable extends JTable
    {
        private final TableColumnAdjuster adjuster = new TableColumnAdjuster(this, 10);
        private final AtomicBoolean columnsAdjusted = new AtomicBoolean();
        private final List<Integer> adjustedWidths = new ArrayList<>();

        void adjustColumns()
        {
            if (!columnsAdjusted.get())
            {
                columnsAdjusted.set(true);
                adjuster.adjustColumns(250);
                int columns = getColumnCount();
                for (int i = 0; i < columns; i++)
                {
                    adjustedWidths.add(getColumnModel().getColumn(i).getWidth());
                }
            }
        }

        void restoreWidths()
        {
            int size = adjustedWidths.size();
            for (int i = 0; i < size; i++)
            {
                TableColumn column = getColumnModel().getColumn(i);
                getTableHeader().setResizingColumn(column);
                column.setWidth(adjustedWidths.get(i));
            }
        }
    }
}
