package com.viskan.payloadbuilder.editor;

import static org.apache.commons.lang3.StringUtils.isBlank;

import java.awt.BorderLayout;
import java.awt.Color;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.util.ArrayList;
import java.util.List;
import java.util.function.Consumer;

import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JSplitPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextArea;
import javax.swing.SwingConstants;
import javax.swing.border.BevelBorder;
import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;
import javax.swing.text.BadLocationException;

import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.time.DurationFormatUtils;
import org.fife.ui.rsyntaxtextarea.SquiggleUnderlineHighlightPainter;
import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rsyntaxtextarea.TextEditorPane;
import org.fife.ui.rtextarea.RTextScrollPane;
import org.kordamp.ikonli.fontawesome.FontAwesome;
import org.kordamp.ikonli.swing.FontIcon;

/** Content of a query editor. Text editor and a result panel separated with a split panel */
class QueryEditorContentView extends JPanel
{
    private final JSplitPane splitPane;
    //    private final JPanel result;
    private final JTextArea messages;
    private final TextEditorPane textEditor;
    private final Consumer<String> textChangeAction;
    private final QueryFile file;
    private final JLabel labelRunTime;
    private final JLabel labelRowCount;

    private boolean resultCollapsed = false;
    private int prevDividerLocation;
    private final JLabel labelExecutionStatus;
    private final AnimatedIcon executionStatusIcon;

    QueryEditorContentView(
            QueryFile file,
            Consumer<String> textChangeAction,
            Consumer<QueryEditorContentView> caretChangeListener)
    {
        this.file = file;
        this.textChangeAction = textChangeAction;
        setLayout(new BorderLayout());

        textEditor = new TextEditorPane();
        textEditor.setColumns(80);
        textEditor.setRows(40);
        textEditor.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_SQL);
        textEditor.setCodeFoldingEnabled(true);
        textEditor.setText(file.getQuery());
        RTextScrollPane sp = new RTextScrollPane(textEditor);
        textEditor.getDocument().addDocumentListener(new TextAreaDocumentListener());
        textEditor.addCaretListener(evt -> caretChangeListener.accept(this));

        messages = new JTextArea();

        JTabbedPane result = new JTabbedPane();
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

        labelExecutionStatus = new JLabel();
        
        executionStatusIcon = new AnimatedIcon(labelExecutionStatus, 
                250,
                FontIcon.of(FontAwesome.HOURGLASS_1),
                FontIcon.of(FontAwesome.HOURGLASS_2),
                FontIcon.of(FontAwesome.HOURGLASS_3));
        
        labelExecutionStatus.setIcon(executionStatusIcon);
        
        labelRunTime = new JLabel("", SwingConstants.CENTER);
        labelRunTime.setPreferredSize(new Dimension(100, 20));
        labelRunTime.setBorder(new BevelBorder(BevelBorder.LOWERED));
        labelRunTime.setToolTipText("Last query run time");

        labelRowCount = new JLabel("", SwingConstants.CENTER);
        labelRowCount.setPreferredSize(new Dimension(100, 20));
        labelRowCount.setBorder(new BevelBorder(BevelBorder.LOWERED));
        labelRowCount.setToolTipText("Last query row count");

        panelStatus.add(labelExecutionStatus);
        panelStatus.add(labelRunTime);
        panelStatus.add(labelRowCount);
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
    
    void startQuery()
    {
        executionStatusIcon.start();
    }

    void stopQuery()
    {
        executionStatusIcon.stop();
    }
    
    void setRunTime(long millis)
    {
        labelRunTime.setText(DurationFormatUtils.formatDurationHMS(millis));
    }

    void setRowCount(int rowCount)
    {
        labelRowCount.setText(String.valueOf(rowCount));
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

    QueryFile getFile()
    {
        return file;
    }

    void setQuery(String text)
    {
        textEditor.setText(text);
    }

    String getQuery(boolean selected)
    {
        return selected && !isBlank(textEditor.getSelectedText()) ? textEditor.getSelectedText() : textEditor.getText();
    }

    void saved()
    {
        textEditor.discardAllEdits();
    }

    void highLight(int line, int column)
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

    void clearHighLights()
    {
        int selStart = textEditor.getSelectionStart();
        int selEnd = textEditor.getSelectionEnd();
        
        textEditor.getHighlighter().removeAllHighlights();
        
        textEditor.setSelectionStart(selStart);
        textEditor.setSelectionEnd(selEnd);
    }

    void setMessage(String message)
    {

        messages.setText(message);
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

    private class TextAreaDocumentListener implements DocumentListener
    {

        @Override
        public void insertUpdate(DocumentEvent e)
        {
            update();
        }

        @Override
        public void removeUpdate(DocumentEvent e)
        {
            update();
        }

        @Override
        public void changedUpdate(DocumentEvent e)
        {
            update();
        }

        private void update()
        {
            textChangeAction.accept(textEditor.getText());
        }
    }
}
