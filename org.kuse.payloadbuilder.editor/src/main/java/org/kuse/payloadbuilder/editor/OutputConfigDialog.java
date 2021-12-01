package org.kuse.payloadbuilder.editor;

import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.GridBagConstraints;
import java.awt.GridBagLayout;
import java.awt.Insets;
import java.awt.Toolkit;

import javax.swing.JButton;
import javax.swing.JCheckBox;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.JTabbedPane;
import javax.swing.JTextField;
import javax.swing.WindowConstants;
import javax.swing.text.AbstractDocument;
import javax.swing.text.AttributeSet;
import javax.swing.text.BadLocationException;
import javax.swing.text.DocumentFilter;

import org.kuse.payloadbuilder.core.CsvOutputWriter.CsvSettings;
import org.kuse.payloadbuilder.core.JsonOutputWriter.JsonSettings;

/** Output config dialog */
class OutputConfigDialog extends JDialog
{
    private final JTabbedPane tabbedPane;
    private final JPanel csvSettings;
    private final JPanel jsonSettings;

    private final JTextField csvEscapeChar;
    private final JTextField csvSeparatorChar;
    private final JTextField csvArrayStartChar;
    private final JTextField csvArrayEndChar;
    private final JTextField csvObjectStartChar;
    private final JTextField csvObjectEndChar;
    private final JCheckBox csvWriteHeaders;
    private final JCheckBox csvEscapeNewLines;
    private final JTextField csvResultSetSeparator;

    private final JTextField jsonRowSeparator;
    private final JTextField jsonResultSetSeparator;
    private final JCheckBox jsonResultSetsAsArrays;
    private final JCheckBox jsonAllResultSetsAsOneArray;
    private final JCheckBox jsonPrettyPrint;

    private boolean cancel;

    OutputConfigDialog(JFrame parent)
    {
        super(parent, true);
        setTitle("Config output");
        getContentPane().setLayout(new BorderLayout());
        setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);

        JButton ok = new JButton("OK");
        ok.addActionListener(l ->
        {
            this.cancel = false;
            this.setVisible(false);
        });
        JButton cancel = new JButton("Cancel");
        cancel.addActionListener(l ->
        {
            this.setVisible(false);
        });

        JPanel bottomPanel = new JPanel();
        bottomPanel.setLayout(new FlowLayout());
        bottomPanel.add(ok);
        bottomPanel.add(cancel);

        getContentPane().add(bottomPanel, BorderLayout.SOUTH);

        /* CSV */

        csvSettings = new JPanel();
        csvSettings.setLayout(new GridBagLayout());
        csvEscapeChar = new PLBTextField(1);
        csvSeparatorChar = new PLBTextField(1);
        csvArrayStartChar = new PLBTextField(1);
        csvArrayEndChar = new PLBTextField(1);
        csvObjectStartChar = new PLBTextField(1);
        csvObjectEndChar = new PLBTextField(1);
        csvWriteHeaders = new JCheckBox();
        csvEscapeNewLines = new JCheckBox();
        csvResultSetSeparator = new JTextField();

        //CSOFF
        csvSettings.add(new JLabel("Escape char"), new GridBagConstraints(0, 0, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 5, 0));
        csvSettings.add(csvEscapeChar, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        csvSettings.add(new JLabel("Separator char"), new GridBagConstraints(0, 1, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 5, 0));
        csvSettings.add(csvSeparatorChar, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        csvSettings.add(new JLabel("Array start char"), new GridBagConstraints(0, 2, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 5, 0));
        csvSettings.add(csvArrayStartChar, new GridBagConstraints(1, 2, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        csvSettings.add(new JLabel("Array end char"), new GridBagConstraints(0, 3, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 5, 0));
        csvSettings.add(csvArrayEndChar, new GridBagConstraints(1, 3, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        csvSettings.add(new JLabel("Object start char"), new GridBagConstraints(0, 4, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 5, 0));
        csvSettings.add(csvObjectStartChar, new GridBagConstraints(1, 4, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        csvSettings.add(new JLabel("Object end char"), new GridBagConstraints(0, 5, 1, 1, 0, 0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 5, 0));
        csvSettings.add(csvObjectEndChar, new GridBagConstraints(1, 5, 1, 1, 1.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        csvSettings.add(new JLabel("Write headers"), new GridBagConstraints(0, 6, 1, 1, 0, 0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        csvSettings.add(csvWriteHeaders, new GridBagConstraints(1, 6, 1, 1, 1.0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        csvSettings.add(new JLabel("Escape new lines"), new GridBagConstraints(0, 7, 1, 1, 0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        csvSettings.add(csvEscapeNewLines, new GridBagConstraints(1, 7, 1, 1, 0.0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        csvSettings.add(new JLabel("Result set separator"), new GridBagConstraints(0, 8, 1, 1, 0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        csvSettings.add(csvResultSetSeparator, new GridBagConstraints(1, 8, 1, 1, 0.0, 0.0, GridBagConstraints.BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        // Fill out last with an empty label
        csvSettings.add(new JLabel(), new GridBagConstraints(0, 8, 1, 1, 0.0, 1.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        /* JSON */
        jsonSettings = new JPanel();
        jsonRowSeparator = new JTextField();
        jsonResultSetSeparator = new JTextField();
        jsonSettings.setLayout(new GridBagLayout());
        jsonResultSetsAsArrays = new JCheckBox();
        jsonAllResultSetsAsOneArray = new JCheckBox();
        jsonPrettyPrint = new JCheckBox();

        jsonSettings.add(new JLabel("Result sets as arrays"),
                new GridBagConstraints(0, 0, 1, 1, 0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        jsonSettings.add(jsonResultSetsAsArrays, new GridBagConstraints(1, 0, 1, 1, 1.0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        jsonSettings.add(new JLabel("ALL result sets as one array"),
                new GridBagConstraints(0, 1, 1, 1, 0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        jsonSettings.add(jsonAllResultSetsAsOneArray, new GridBagConstraints(1, 1, 1, 1, 1.0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        jsonSettings.add(new JLabel("Pretty print"), new GridBagConstraints(0, 2, 1, 1, 0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        jsonSettings.add(jsonPrettyPrint, new GridBagConstraints(1, 2, 1, 1, 1.0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));

        jsonSettings.add(new JLabel("Row separator"), new GridBagConstraints(0, 3, 1, 1, 0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        jsonSettings.add(jsonRowSeparator, new GridBagConstraints(1, 3, 1, 1, 1.0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        jsonSettings.add(new JLabel("Result set separator"),
                new GridBagConstraints(0, 4, 1, 1, 0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        jsonSettings.add(jsonResultSetSeparator, new GridBagConstraints(1, 4, 1, 1, 1.0, 0.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.HORIZONTAL, new Insets(0, 0, 0, 0), 0, 0));

        // Fill out last with an empty label
        jsonSettings.add(new JLabel(), new GridBagConstraints(0, 5, 1, 1, 0.0, 1.0, GridBagConstraints.BELOW_BASELINE_LEADING, GridBagConstraints.NONE, new Insets(0, 0, 0, 0), 0, 0));
        //CSON

        tabbedPane = new JTabbedPane();
        tabbedPane.add("CSV", csvSettings);
        tabbedPane.add("JSON", jsonSettings);

        getContentPane().add(new JScrollPane(tabbedPane), BorderLayout.CENTER);

        setPreferredSize(PayloadbuilderEditorView.DEFAULT_DIALOG_SIZE);
        pack();
        setLocationRelativeTo(null);
    }

    void setSettings(Config config)
    {
        CsvSettings csvSettings = config.getOutputConfig().getCsvSettings();
        setChar(csvEscapeChar, csvSettings::setEscapeChar);
        setChar(csvSeparatorChar, csvSettings::setSeparatorChar);
        setChar(csvArrayStartChar, csvSettings::setArrayStartChar);
        setChar(csvArrayEndChar, csvSettings::setArrayEndChar);
        setChar(csvObjectStartChar, csvSettings::setObjectStartChar);
        setChar(csvObjectEndChar, csvSettings::setObjectEndChar);
        csvSettings.setWriteHeaders(csvWriteHeaders.isSelected());
        csvSettings.setEscapeNewLines(csvEscapeNewLines.isSelected());
        csvSettings.setResultsetSeparator(csvResultSetSeparator.getText());

        JsonSettings jsonSettings = config.getOutputConfig().getJsonSettings();
        jsonSettings.setRowSeparator(jsonRowSeparator.getText());
        jsonSettings.setResultSetSeparator(jsonResultSetSeparator.getText());
        jsonSettings.setResultSetsAsArrays(jsonResultSetsAsArrays.isSelected());
        jsonSettings.setAllResultSetsAsOneArray(jsonAllResultSetsAsOneArray.isSelected());
        jsonSettings.setPrettyPrint(jsonPrettyPrint.isSelected());
    }

    /** Init dialog */
    void init(Config config)
    {
        cancel = true;
        CsvSettings csvSettings = config.getOutputConfig().getCsvSettings();

        csvEscapeChar.setText(String.valueOf(csvSettings.getEscapeChar()));
        csvSeparatorChar.setText(String.valueOf(csvSettings.getSeparatorChar()));
        csvArrayStartChar.setText(String.valueOf(csvSettings.getArrayStartChar()));
        csvArrayEndChar.setText(String.valueOf(csvSettings.getArrayEndChar()));
        csvObjectStartChar.setText(String.valueOf(csvSettings.getObjectStartChar()));
        csvObjectEndChar.setText(String.valueOf(csvSettings.getObjectEndChar()));
        csvWriteHeaders.setSelected(csvSettings.isWriteHeaders());
        csvEscapeNewLines.setSelected(csvSettings.isEscapeNewLines());
        csvResultSetSeparator.setText(csvSettings.getResultsetSeparator());

        JsonSettings jsonSettings = config.getOutputConfig().getJsonSettings();
        jsonRowSeparator.setText(jsonSettings.getRowSeparator());
        jsonResultSetSeparator.setText(jsonSettings.getResultSetSeparator());
        jsonResultSetsAsArrays.setSelected(jsonSettings.isResultSetsAsArrays());
        jsonAllResultSetsAsOneArray.setSelected(jsonSettings.isAllResultSetsAsOneArray());
        jsonPrettyPrint.setSelected(jsonSettings.isPrettyPrint());
    }

    public boolean isOk()
    {
        return !cancel;
    }

    //    Map<String, Object> getVariables()
    //    {
    //        if (cancel)
    //        {
    //            return null;
    //        }
    //        try
    //        {
    //            return MAPPER.readValue(textEditor.getText().replace("\\R+", ""), Map.class);
    //        }
    //        catch (JsonProcessingException e)
    //        {
    //            return null;
    //        }
    //    }

    /** Text field with max length */
    private static class PLBTextField extends JTextField
    {
        PLBTextField(final int maxChars)
        {
            if (this.getDocument() instanceof AbstractDocument)
            {
                //CSOFF
                ((AbstractDocument) this.getDocument()).setDocumentFilter(new DocumentFilter()
                //CSON
                {
                    @Override
                    public void insertString(FilterBypass fb, int offset, String string, AttributeSet attr) throws BadLocationException
                    {
                        if (!ignore(fb, string))
                        {
                            super.insertString(fb, offset, string, attr);
                        }
                        else
                        {
                            Toolkit.getDefaultToolkit().beep();
                        }
                    }

                    @Override
                    public void replace(FilterBypass fb, int offset, int length, String text, AttributeSet attrs) throws BadLocationException
                    {
                        if (!ignore(fb, text))
                        {
                            super.replace(fb, offset, length, text, attrs);
                        }
                    }

                    private boolean ignore(FilterBypass fb, String string)
                    {
                        return (fb.getDocument().getLength() + string.length()) > maxChars;
                    }
                });
            }
        }
    }

    void setChar(JTextField tf, CharConsumer consumer)
    {
        char value = tf.getText().length() > 0 ? tf.getText().charAt(0) : Character.MIN_VALUE;
        consumer.apply(value);
    }

    /** Char consumer */
    @FunctionalInterface
    private interface CharConsumer
    {
        void apply(char value);
    }
}
