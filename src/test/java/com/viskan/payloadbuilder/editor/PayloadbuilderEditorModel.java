package com.viskan.payloadbuilder.editor;

import com.viskan.payloadbuilder.provider.elastic.EtmArticleCategoryESCatalogExtension;

import java.beans.PropertyChangeListener;
import java.util.ArrayList;
import java.util.List;

import javax.swing.event.SwingPropertyChangeSupport;

class PayloadbuilderEditorModel
{
    private final SwingPropertyChangeSupport pcs = new SwingPropertyChangeSupport(this);
    public static final String FILES = "files";
    public static final String SELECTED_FILE = "selectedFile";

    private final List<QueryFileModel> files = new ArrayList<>();
    private final List<CatalogExtensionModel> extensions = new ArrayList<>();

    PayloadbuilderEditorModel()
    {
        extensions.add(new CatalogExtensionModel(new EtmArticleCategoryESCatalogExtension()));
    }
    
    void addPropertyChangeListener(PropertyChangeListener listener)
    {
        pcs.addPropertyChangeListener(listener);
    }

    void addFile(QueryFileModel file)
    {
        setSelectedFile(files.size(), file);
    }
    
    void setSelectedFile(int index)
    {
        setSelectedFile(index, files.get(index));
    }

    void setSelectedFile(int index, QueryFileModel file)
    {
        QueryFileModel existing = index == files.size() ? null : files.get(index);
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
    
    List<CatalogExtensionModel> getExtensions()
    {
        return extensions;
    }

    List<QueryFileModel> getFiles()
    {
        return files;
    }

    void removeFile(QueryFileModel file)
    {
        files.remove(file);
    }
}
