package com.viskan.payloadbuilder.editor;

import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;
import java.util.Observable;

import javax.swing.event.SwingPropertyChangeSupport;

class PayloadbuilderEditorModel extends Observable
{
    private final SwingPropertyChangeSupport pcs = new SwingPropertyChangeSupport(this);
    public static final String FILES = "files";
    public static final String SELECTED_FILE = "selectedFile";

    private final List<QueryFile> files = new ArrayList<>();

    void addPropertyChangeListener(PropertyChangeListener listener)
    {
        pcs.addPropertyChangeListener(listener);
    }

    void addFile(QueryFile file)
    {
        setSelectedFile(files.size(), file);
    }
    
    void setSelectedFile(int index)
    {
        setSelectedFile(index, files.get(index));
    }

    void setSelectedFile(int index, QueryFile file)
    {
        QueryFile existing = index == files.size() ? null : files.get(index);
        if (existing != null)
        {
            if (existing.equals(file))
            {
                return;
            }
            files.remove(index);
            files.add(index, file);
        }
        else
        {
            files.add(file);
        }
        pcs.fireIndexedPropertyChange(SELECTED_FILE, index, existing, file);
    }

    List<QueryFile> getFiles()
    {
        return files;
    }

    void removeFile(QueryFile file)
    {
        files.remove(file);
    }
}
