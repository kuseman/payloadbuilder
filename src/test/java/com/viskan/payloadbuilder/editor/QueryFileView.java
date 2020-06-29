package com.viskan.payloadbuilder.editor;

import com.viskan.payloadbuilder.editor.QueryFileModel.State;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Component;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.awt.Point;
import java.awt.event.MouseAdapter;
import java.awt.event.MouseEvent;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import javax.swing.Icon;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTable;
import javax.swing.JTextArea;
import javax.swing.SwingConstants;
import javax.swing.Timer;
import javax.swing.WindowConstants;
import javax.swing.event.TableModelEvent;
import javax.swing.table.DefaultTableCellRenderer;
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

/** Content of a query editor. Text editor and a result panel separated with a split panel */
class QueryFileView extends JPanel
{
    private final JSplitPane splitPane;
    //    private final JPanel result;
    private final TextEditorPane textEditor;
    private final JTextArea messages;
    private final JTable resultTable;
    private final QueryFileModel file;
    private final JLabel labelRunTime;
    private final JLabel labelRowCount;
    private final Timer executionTimer;

    private final ResultModel resultModel;

    private boolean columnsAdjusted;
    private boolean resultCollapsed;
    private int prevDividerLocation;

    private final static Icon CHECK_CIRCLE_ICON = FontIcon.of(FontAwesome.CHECK_CIRCLE);
    private final static Icon PLAY_ICON = FontIcon.of(FontAwesome.PLAY);
    private final static Icon CLOSE_ICON = FontIcon.of(FontAwesome.CLOSE);
    private final static Icon WARNING_ICON = FontIcon.of(FontAwesome.WARNING);

    QueryFileView(
            QueryFileModel file,
            Consumer<String> textChangeAction,
            Consumer<QueryFileView> caretChangeListener)
    {
        this.file = file;
        setLayout(new BorderLayout());

        textEditor = new TextEditorPane();
        textEditor.setColumns(80);
        textEditor.setRows(40);
        textEditor.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_SQL);
        textEditor.setCodeFoldingEnabled(true);
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
        resultTable = createResultTable();
        TableColumnAdjuster columnAdjuster = new TableColumnAdjuster(resultTable, 10);
        resultModel = new ResultModel(file);
        // Add listener to adjust columns upon reaching 30 rows or query is complete
        resultModel.addTableModelListener(l ->
        {
            if (l.getType() == TableModelEvent.INSERT
                && !columnsAdjusted
                && (resultModel.getRowCount() > 30 || file.getState() != State.EXECUTING))
            {
                columnAdjuster.adjustColumns();
                columnsAdjusted = true;
            }

        });
        resultTable.setModel(resultModel);

        JTabbedPane result = new JTabbedPane();
        result.add("Result", new JScrollPane(resultTable));
        result.add("Messages", new JScrollPane(messages));

        splitPane = new JSplitPane();
        splitPane.setOrientation(JSplitPane.VERTICAL_SPLIT);
        splitPane.setDividerLocation(400);
        splitPane.setLeftComponent(sp);
        splitPane.setRightComponent(result);
        add(splitPane, BorderLayout.CENTER);

        JPanel panelStatus = new JPanel();
        panelStatus.setPreferredSize(new Dimension(10, 20));
        add(panelStatus, BorderLayout.SOUTH);
        panelStatus.setLayout(new FlowLayout(FlowLayout.LEFT, 5, 0));

        JLabel labelExecutionStatus = new JLabel(getIconFromState(file.getState()));

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
                labelExecutionStatus.setIcon(getIconFromState(file.getState()));
                labelExecutionStatus.setToolTipText(file.getState().getToolTip());

                switch (file.getState())
                {
                    case EXECUTING:
                        resultModel.clear();
                        messages.setText("");
                        executionTimer.start();
                        result.setSelectedIndex(0);
                        columnsAdjusted = false;
                        clearHighLights();
                        break;
                    case COMPLETED:
                        setExecutionStats();
                        executionTimer.stop();
                        break;
                    case ABORTED:
                        setExecutionStats();
                        messages.setText("Query was aborted!");
                        executionTimer.stop();
                        break;
                    case ERROR:
                        result.setSelectedIndex(1);
                        messages.setText(file.getError());
                        if (file.getParseErrorLocation() != null)
                        {
                            highLight(file.getParseErrorLocation().getKey(), file.getParseErrorLocation().getValue());
                        }
                        break;
                }
            }
        });
    }

    private JTable createResultTable()
    {
        JTable result = new JTable();
        result.setAutoResizeMode(JTable.AUTO_RESIZE_OFF);
        result.setDefaultRenderer(Object.class, new DefaultTableCellRenderer()
        {
            @Override
            public Component getTableCellRendererComponent(JTable table, Object value, boolean isSelected, boolean hasFocus, int row, int column)
            {
                super.getTableCellRendererComponent(table, value, isSelected, hasFocus, row, column);

                // Add icon to map/list results to indicate click-ability
                if (value instanceof Collection || value instanceof Map)
                {
                    setIcon(FontIcon.of(FontAwesome.EXTERNAL_LINK));
                    setText(ResultModel.getLabel(value, 10));
                    setHorizontalTextPosition(SwingConstants.TRAILING);
                    setAlignmentX(SwingConstants.LEFT);
                }
                else
                {
                    setIcon(null);
                }
                return this;
            }
        });
        result.addMouseListener(new MouseAdapter()
        {
            @Override
            public void mouseClicked(MouseEvent e)
            {
                if (e.getClickCount() == 2 && e.getButton() == MouseEvent.BUTTON1)
                {
                    Point point = e.getPoint();
                    int row = resultTable.rowAtPoint(point);
                    int col = resultTable.columnAtPoint(point);

                    Object value = resultTable.getValueAt(row, col);
                    if (value instanceof Collection || value instanceof Map)
                    {
                        JFrame frame = new JFrame("Json viewer - " + resultTable.getColumnName(col) + " (Row: " + (row + 1) + ")");
                        RSyntaxTextArea rta = new RSyntaxTextArea();
                        rta.setColumns(80);
                        rta.setRows(40);
                        rta.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_JSON);
                        rta.setCodeFoldingEnabled(true);
                        rta.setText(ResultModel.getPrettyJson(value));
                        rta.setEditable(false);
                        RTextScrollPane sp = new RTextScrollPane(rta);
                        frame.getContentPane().add(sp);
                        frame.setSize(600, 400);
                        frame.setDefaultCloseOperation(WindowConstants.DISPOSE_ON_CLOSE);
                        frame.setVisible(true);
                    }
                }
            }
        });
        return result;
    }

    ResultModel getResultModel()
    {
        return resultModel;
    }

    private Icon getIconFromState(State state)
    {
        switch (state)
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
        labelRowCount.setText(String.valueOf(resultModel.getActualRowCOunt()));
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
            int pos = textEditor.getLineStartOffset(line - 1) + column - 1;
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
}
