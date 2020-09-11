/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.editor;

import java.awt.BorderLayout;
import java.awt.Dimension;
import java.awt.FlowLayout;
import java.util.Map;

import javax.swing.JButton;
import javax.swing.JDialog;
import javax.swing.JFrame;
import javax.swing.JPanel;
import javax.swing.JScrollPane;
import javax.swing.WindowConstants;

import org.fife.ui.rsyntaxtextarea.SyntaxConstants;
import org.fife.ui.rsyntaxtextarea.TextEditorPane;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Variables dialog */
class VariablesDialog extends JDialog
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private final TextEditorPane textEditor;
    private boolean cancel;

    VariablesDialog(JFrame parent)
    {
        super(parent, true);
        getContentPane().setLayout(new BorderLayout());
        setDefaultCloseOperation(WindowConstants.HIDE_ON_CLOSE);

        JButton ok = new JButton("OK");
        ok.addActionListener(l ->
        {
            this.setVisible(false);
        });
        JButton cancel = new JButton("Cancel");
        cancel.addActionListener(l ->
        {
            this.cancel = true;
            this.setVisible(false);
        });

        JPanel bottomPanel = new JPanel();
        bottomPanel.setLayout(new FlowLayout());
        bottomPanel.add(ok);
        bottomPanel.add(cancel);

        getContentPane().add(bottomPanel, BorderLayout.SOUTH);

        textEditor = new TextEditorPane();
        textEditor.setSyntaxEditingStyle(SyntaxConstants.SYNTAX_STYLE_JSON);
        textEditor.setCodeFoldingEnabled(true);
        textEditor.setBracketMatchingEnabled(true);
        textEditor.setCaretPosition(0);

        getContentPane().add(new JScrollPane(textEditor), BorderLayout.CENTER);

        setPreferredSize(new Dimension(800, 600));
        pack();
        setLocationRelativeTo(null);
    }

    /** Init dialog */
    void init(String title, Map<String, Object> existingVariables)
    {
        setTitle("Edit variables: " + title);
        cancel = false;
        try
        {
            textEditor.setText(ResultModel.WRITER.writeValueAsString(existingVariables));
        }
        catch (JsonProcessingException e)
        {
        }
    }

    Map<String, Object> getVariables()
    {
        if (cancel)
        {
            return null;
        }
        try
        {
            return MAPPER.readValue(textEditor.getText().replace("\\R+", ""), Map.class);
        }
        catch (JsonProcessingException e)
        {
            return null;
        }
    }
}
