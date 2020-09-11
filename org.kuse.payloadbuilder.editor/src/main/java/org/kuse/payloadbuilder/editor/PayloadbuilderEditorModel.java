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

    List<QueryFileModel> getFiles()
    {
        return files;
    }

    void removeFile(QueryFileModel file)
    {
        files.remove(file);
    }
}
