package org.kuse.payloadbuilder.editor;

import javax.swing.event.DocumentEvent;
import javax.swing.event.DocumentListener;

/** Document listener adapter */
public abstract class ADocumentListenerAdapter implements DocumentListener
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

    protected abstract void update();
}
