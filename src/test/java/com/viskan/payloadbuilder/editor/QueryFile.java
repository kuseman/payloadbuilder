package com.viskan.payloadbuilder.editor;

import java.beans.PropertyChangeListener;
import java.io.File;
import java.io.IOException;
import java.util.Objects;

import javax.swing.event.SwingPropertyChangeSupport;

import org.apache.commons.io.FileUtils;

public class QueryFile
{
    private final SwingPropertyChangeSupport pcs = new SwingPropertyChangeSupport(this);
    static final String DIRTY = "dirty";
    static final String FILENAME = "filename";
    static final String QUERY = "query";
    
    private boolean dirty;
    private boolean newFile = true;
    private String filename = "";
    /** Query from disk */
    private String savedQuery = "";
    private String query = "";
   
    QueryFile()
    {}
    
    /** Initialize a file from filename */
    QueryFile(File file)
    {
        this.filename = file.getAbsolutePath();
        try
        {
            query = FileUtils.readFileToString(file);
        }
        catch (IOException e)
        {
            throw new RuntimeException(e);
        }
        dirty = false;
        newFile = false;
        savedQuery = query;
    }
    
    boolean isDirty()
    {
        return dirty;
    }

    void setDirty(boolean dirty)
    {
        boolean oldValue = this.dirty;
        boolean newValue = dirty;
        if (newValue != oldValue)
        {
            this.dirty = dirty;
            pcs.firePropertyChange(DIRTY, oldValue, newValue);
        }
    }

    boolean isNew()
    {
        return newFile;
    }
    
    void setNew(boolean newFile)
    {
        this.newFile = newFile;
    }
    
    String getFilename()
    {
        return filename;
    }
    
    void setFilename(String filename)
    {
        String newValue = filename;
        String oldValue = this.filename;
        if (!newValue.equals(oldValue))
        {
            pcs.firePropertyChange(FILENAME, oldValue, newValue);
            this.filename = filename;
        }
    }
    
    /** Saves file to disk */
    void save()
    {
        try
        {
            FileUtils.write(new File(filename), query);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Unable to save file " + filename, e);
        }
        newFile = false;
        savedQuery = query;
        setDirty(false);
    }
    
    String getQuery()
    {
        return query;
    }
    
    void setQuery(String query)
    {
        String newValue = query;
        String oldValue = this.query;
        if (!newValue.equals(oldValue))
        {
            pcs.firePropertyChange(QUERY, oldValue, newValue);
            this.query = query;
        }
        
        setDirty(!Objects.equals(query, savedQuery));
    }
    
    void addPropertyChangeListener(PropertyChangeListener listener)
    {
        pcs.addPropertyChangeListener(listener);
    }
}
