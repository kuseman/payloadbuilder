package com.viskan.payloadbuilder.editor;

import static java.util.Collections.emptyMap;
import static org.apache.commons.lang3.ObjectUtils.defaultIfNull;

import java.beans.PropertyChangeListener;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import javax.swing.event.SwingPropertyChangeSupport;

class PayloadbuilderEditorModel
{
    private final SwingPropertyChangeSupport pcs = new SwingPropertyChangeSupport(this);
    public static final String FILES = "files";
    public static final String SELECTED_FILE = "selectedFile";

    private final List<QueryFileModel> files = new ArrayList<>();
    private final List<ICatalogExtension> extensions = new ArrayList<>();

    PayloadbuilderEditorModel(Map<String, Object> config)
    {
        @SuppressWarnings("unchecked")
        Map<String, Object> catalogConfig = defaultIfNull((Map<String, Object>) config.get(PayloadbuilderEditorController.CATALOG_CONFIG), emptyMap());
        for (String configClass : catalogConfig.keySet())
        {
            try
            {
                Class<?> clazz = Class.forName(configClass);
                if (ICatalogExtension.class.isAssignableFrom(clazz))
                {
                    Constructor<?> ctor = clazz.getDeclaredConstructors()[0];
                    ctor.setAccessible(true);
                    extensions.add((ICatalogExtension) ctor.newInstance());
                }
            }
            catch (Exception e)
            {
                // TODO: log bus
                throw new RuntimeException("Cannot instansiate extension " + configClass, e);
            }
        }
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
    
    List<ICatalogExtension> getCatalogExtensions()
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
