package com.viskan.payloadbuilder.editor;

import javax.swing.SwingUtilities;
import javax.swing.UIManager;
import javax.swing.UnsupportedLookAndFeelException;

public class Main
{
    public static void main(String[] args)
    {
        try
        {
            UIManager.setLookAndFeel(UIManager.getSystemLookAndFeelClassName());
        }
        catch (ClassNotFoundException | InstantiationException | IllegalAccessException | UnsupportedLookAndFeelException ex)
        {
        }

        // Start all Swing applications on the EDT.
        SwingUtilities.invokeLater(() ->
        {
            PayloadbuilderEditorView view = new PayloadbuilderEditorView();
            PayloadbuilderEditorModel model = new PayloadbuilderEditorModel();
            new PayloadbuilderEditorController(view, model);

            view.setVisible(true);
            //            new PayloadbuilderEditorView().setVisible(true);
        });
    }

}
