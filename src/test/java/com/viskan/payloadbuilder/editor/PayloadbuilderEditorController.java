package com.viskan.payloadbuilder.editor;

import com.viskan.payloadbuilder.editor.QueryFileModel.Output;
import com.viskan.payloadbuilder.editor.QueryFileModel.State;
import com.viskan.payloadbuilder.editor.catalog.ICatalogExtension;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.util.function.Consumer;

import javax.swing.ButtonGroup;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.JScrollPane;
import javax.swing.Timer;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

/** Main controller for editor */
class PayloadbuilderEditorController implements PropertyChangeListener
{
    private final PayloadbuilderEditorView view;
    private final PayloadbuilderEditorModel model;
    private final CaretChangedListener caretChangedListener = new CaretChangedListener();
    private int newFileCounter = 1;

    PayloadbuilderEditorController(PayloadbuilderEditorView view, PayloadbuilderEditorModel model)
    {
        this.view = requireNonNull(view, "view");
        this.model = requireNonNull(model, "model");
        this.model.addPropertyChangeListener(this);
        init();
    }

    private void init()
    {
        view.setExecuteAction(new ExecuteListener());
        view.setCancelAction(() ->
        {
            QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null && editor.getFile().getState() == State.EXECUTING)
            {
                editor.getFile().setState(State.ABORTED);
            }
        });
        view.setNewQueryAction(new NewQueryListener());
        //        view.setFormatAction(new FormatListener());
        view.setSaveAction(new SaveListener());
        view.setOpenAction(new OpenListener());
        view.setToogleResultAction(() ->
        {
            QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                editor.toggleResultPane();
            }
        });
        view.setToggleCommentRunnable(() ->
        {
            QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                editor.toggleComments();
            }
        });
        view.setOutputChangedAction(() ->
        {
            QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                editor.getFile().setOutput((Output) view.getOutputCombo().getSelectedItem());
            }
        });

        ButtonGroup defaultGroup = new ButtonGroup();

        for (ICatalogExtension extension : model.getExtensions())
        {
            view.getPanelCatalogs().add(new JScrollPane(new CatalogExtensionView(extension, defaultGroup)));
            //            {
            //                QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
            //                if (editor != null)
            //                {
            //                    QueryFileModel file = editor.getFile();
            //                    file.getCatalogValues()
            //                        .computeIfAbsent(extension, key -> new HashMap<>())
            //                        .put(item, value);
            //                }
            //            }));
        }

        view.getEditorsTabbedPane().addChangeListener(new SelectedFileListener());
        view.setExitAction(() -> System.exit(0));

        view.getMemoryLabel().setText(getMemoryString());
        new Timer(250, evt -> view.getMemoryLabel().setText(getMemoryString())).start();
    }

    private String getMemoryString()
    {
        Runtime runtime = Runtime.getRuntime();
        return String.format("%s / %s", byteCountToDisplaySize(runtime.totalMemory()), byteCountToDisplaySize(runtime.freeMemory()));
    }

    private boolean save(QueryFileModel file)
    {
        if (!file.isDirty())
        {
            return true;
        }

        if (file.isNew())
        {
            JFileChooser fileChooser = new JFileChooser()
            {
                @Override
                public void approveSelection()
                {
                    File f = getSelectedFile();
                    if (f.exists() && getDialogType() == SAVE_DIALOG)
                    {
                        int result = JOptionPane.showConfirmDialog(this, "The file exists, overwrite?", "Existing file", JOptionPane.YES_NO_CANCEL_OPTION);
                        switch (result)
                        {
                            case JOptionPane.YES_OPTION:
                                super.approveSelection();
                                return;
                            case JOptionPane.NO_OPTION:
                                return;
                            case JOptionPane.CLOSED_OPTION:
                                return;
                            case JOptionPane.CANCEL_OPTION:
                                cancelSelection();
                                return;
                        }
                    }
                    super.approveSelection();
                }
            };
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
            QueryFileModel file = (QueryFileModel) evt.getNewValue();
            // New tab
            if (evt.getOldValue() == null)
            {
                QueryFileView content = new QueryFileView(
                        file,
                        text -> file.setQuery(text),
                        caretChangedListener);
                view.getEditorsTabbedPane().add(content);

                TabComponentView tabComponent = new TabComponentView(file.getTabTitle(), null, () ->
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

                    // Find tab index with "this" content
                    int length = view.getEditorsTabbedPane().getComponents().length;
                    for (int i = 0; i < length; i++)
                    {
                        if (view.getEditorsTabbedPane().getComponents()[i] == content)
                        {
                            model.removeFile(file);
                            view.getEditorsTabbedPane().remove(content);
                            break;
                        }
                    }
                });
                
                file.addPropertyChangeListener(l ->
                {
                    tabComponent.setTitle(file.getTabTitle());
                });

                view.getEditorsTabbedPane().setTabComponentAt(model.getFiles().size() - 1, tabComponent);
                view.getEditorsTabbedPane().setSelectedIndex(model.getFiles().size() - 1);
                content.requestFocusInWindow();
            }
            // Set selected
            else
            {
            }
        }
    };

    private class SelectedFileListener implements ChangeListener
    {
        @Override
        public void stateChanged(ChangeEvent e)
        {
            int index = view.getEditorsTabbedPane().getSelectedIndex();
            if (index >= 0)
            {
                QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
                caretChangedListener.accept(editor);
                model.setSelectedFile(index);
                view.getOutputCombo().setSelectedItem(editor.getFile().getOutput());
            }
        }
    }

    private class ExecuteListener implements Runnable
    {
        @Override
        public void run()
        {
            QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor == null)
            {
                return;
            }
            String queryString = editor.getQuery(true);
            if (isBlank(queryString))
            {
                return;
            }
            
            if (editor.getFile().getState() == State.EXECUTING)
            {
                return;
            }

            PayloadbuilderService.executeQuery(editor.getFile(), queryString);
        }
    }

    private class CaretChangedListener implements Consumer<QueryFileView>
    {
        @Override
        public void accept(QueryFileView t)
        {
            view.getCaretLabel().setText(String.format("%d : %d : %d", t.getCaretLineNumber(), t.getCaretOffsetFromLineStart(), t.getCaretPosition()));
        }
    }

    private class NewQueryListener implements Runnable
    {
        @Override
        public void run()
        {
            QueryFileModel queryFile = new QueryFileModel();
            queryFile.setFilename("Query" + (newFileCounter++) + ".sql");
            model.addFile(queryFile);
        }
    }

    //    private class FormatListener implements Runnable
    //    {
    //        @Override
    //        public void run()
    //        {
    //            QueryEditorContentView editor = (QueryEditorContentView) view.getEditorsTabbedPane().getSelectedComponent();
    //            if (editor != null)
    //            {
    //                String queryString = editor.getQuery(false);
    //                if (!isBlank(queryString))
    //                {
    //                    //                    System.out.println("FORMAT " + query);
    //                }
    //            }
    //        }
    //    }

    private class OpenListener implements Runnable
    {
        @Override
        public void run()
        {
            JFileChooser fileChooser = new JFileChooser();
            if (fileChooser.showOpenDialog(view) == JFileChooser.APPROVE_OPTION)
            {
                QueryFileModel file = new QueryFileModel(fileChooser.getSelectedFile());
                model.addFile(file);
            }
        }
    }

    private class SaveListener implements Runnable
    {
        @Override
        public void run()
        {
            QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                QueryFileModel file = editor.getFile();
                save(file);
            }
        }
    }
}
