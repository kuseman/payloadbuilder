package com.viskan.payloadbuilder.editor;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

/** Main controller for editor */
public class PayloadbuilderEditorController implements PropertyChangeListener
{
    private final PayloadbuilderEditorView view;
    private final PayloadbuilderEditorModel model;

    public PayloadbuilderEditorController(PayloadbuilderEditorView view, PayloadbuilderEditorModel model)
    {
        this.view = requireNonNull(view, "view");
        this.model = requireNonNull(model, "model");
        this.model.addPropertyChangeListener(this);
        init();
    }

    private void init()
    {
        view.setExecuteAction(new ExecuteListener());
        view.setNewQueryAction(new NewQueryListener());
        view.setFormatAction(new FormatListener());
        view.setSaveAction(new SaveListener());
        view.setOpenAction(new OpenListener());
        view.setToogleResultAction(() ->
        {
            QueryEditorContent editor = (QueryEditorContent) view.getFileTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                editor.toggleResultPane();
            }
        });
        view.setToggleCommentRunnable(() -> 
        {
            QueryEditorContent editor = (QueryEditorContent) view.getFileTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                editor.toggleComments();
            }
        });
        
        view.getFileTabbedPane().addChangeListener(new SelectFileListener());
        view.setExitAction(() -> System.exit(0));
    }

    private class SelectFileListener implements ChangeListener
    {
        @Override
        public void stateChanged(ChangeEvent e)
        {
            int index = view.getFileTabbedPane().getSelectedIndex();
            if (index >= 0)
            {
                model.setSelectedFile(index);
            }
        }
    }

    private class ExecuteListener implements Runnable
    {
        @Override
        public void run()
        {
            QueryEditorContent editor = (QueryEditorContent) view.getFileTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                String query = editor.getQuery(true);
                if (!isBlank(query))
                {
                    System.out.println("EXECUTE " + query);
                }
            }
        }
    }

    private class NewQueryListener implements Runnable
    {
        @Override
        public void run()
        {
            QueryFile queryFile = new QueryFile();
            queryFile.setFilename("Query" + (model.getFiles().size() + 1) + ".sql");
            model.addFile(queryFile);
        }
    }

    private class FormatListener implements Runnable
    {
        @Override
        public void run()
        {
            QueryEditorContent editor = (QueryEditorContent) view.getFileTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                String query = editor.getQuery(false);
                if (!isBlank(query))
                {
                    System.out.println("FORMAT " + query);
                }
            }
        }
    }

    private class OpenListener implements Runnable
    {
        @Override
        public void run()
        {
            JFileChooser fileChooser = new JFileChooser();
            if (fileChooser.showOpenDialog(view) == JFileChooser.APPROVE_OPTION)
            {
                QueryFile file = new QueryFile(fileChooser.getSelectedFile());
                model.addFile(file);
            }
        }
    }

    private class SaveListener implements Runnable
    {
        @Override
        public void run()
        {
            QueryEditorContent editor = (QueryEditorContent) view.getFileTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                QueryFile file = editor.getFile();
                save(file);
            }
        }
    }
    
    private boolean save(QueryFile file)
    {
        if (file.isNew())
        {
            JFileChooser fileChooser = new JFileChooser();
            fileChooser.setSelectedFile(new File(file.getFilename()));
            int result = fileChooser.showSaveDialog(view);
            if (result == JFileChooser.APPROVE_OPTION)
            {
                file.setFilename(fileChooser.getSelectedFile().getAbsolutePath());
            }
            else if (result == JFileChooser.CANCEL_OPTION)
            {
                return false;
            }
        }

        file.save();
        return true;
    }

    @Override
    public void propertyChange(PropertyChangeEvent evt)
    {
        if (PayloadbuilderEditorModel.SELECTED_FILE.equals(evt.getPropertyName()))
        {
            QueryFile file = (QueryFile) evt.getNewValue();
            // New tab
            if (evt.getOldValue() == null)
            {
                QueryEditorContent content = new QueryEditorContent(file, text -> file.setQuery(text));
                view.getFileTabbedPane().add(content);//.requestFocus();
                view.getFileTabbedPane().setTabComponentAt(model.getFiles().size() - 1, new TabComponent(file, () ->  
                {
                    if (file.isDirty())
                    {
                        int result = JOptionPane.showConfirmDialog(view, "Save changes ?", "Save", JOptionPane.YES_NO_CANCEL_OPTION);
                        if (result == JOptionPane.CANCEL_OPTION)
                        {
                            return;
                        }
                        else if (result == JOptionPane.YES_OPTION)
                        {
                            if (!save(file))
                            {
                                return;
                            }
                        }
                    }
                    
                    int length = view.getFileTabbedPane().getComponents().length;
                    for (int i=0;i<length;i++)
                    {
                        if (view.getFileTabbedPane().getComponents()[i] == content)
                        {
                            model.removeFile(file);
                            view.getFileTabbedPane().remove(content);
                            break;
                        }
                    }
                }));
                view.getFileTabbedPane().setSelectedIndex(model.getFiles().size() - 1);
                content.requestFocusInWindow();
            }
            // Set selected
            else
            {
            }
        }
    };
}
