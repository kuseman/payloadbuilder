package org.kuse.payloadbuilder.editor;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.awt.GridBagConstraints;
import java.awt.Insets;
import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Consumer;

import javax.swing.ButtonGroup;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.Timer;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.apache.commons.io.FilenameUtils;
import org.kuse.payloadbuilder.editor.QueryFileModel.Output;
import org.kuse.payloadbuilder.editor.QueryFileModel.State;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Main controller for editor */
class PayloadbuilderEditorController implements PropertyChangeListener
{
    static final ObjectMapper MAPPER = new ObjectMapper()
            .configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
    private final PayloadbuilderEditorView view;
    private final PayloadbuilderEditorModel model;
    private final VariablesDialog variablesDialog;
    private final CaretChangedListener caretChangedListener = new CaretChangedListener();
    private final Config config;
    private final List<CatalogExtensionView> catalogExtensionViews = new ArrayList<>();
    private final ButtonGroup defaultGroup = new ButtonGroup();
    private int newFileCounter = 1;

    PayloadbuilderEditorController(
            Config config,
            PayloadbuilderEditorView view,
            PayloadbuilderEditorModel model)
    {
        this.config = config;
        this.view = requireNonNull(view, "view");
        this.model = requireNonNull(model, "model");
        this.model.addPropertyChangeListener(this);
        this.variablesDialog = new VariablesDialog(view);
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
        view.setEditVariablesRunnable(new EditVariablesListener());
        view.setOutputChangedAction(() ->
        {
            QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                editor.getFile().setOutput((Output) view.getOutputCombo().getSelectedItem());
            }
        });

        int y = 0;
        Insets insets = new Insets(0, 0, 3, 0);
        for (Config.Catalog catalog : config.getCatalogs())
        {
            ICatalogExtension extension = catalog.getCatalogExtension();
            extension.load(catalog.getConfig());

            CatalogExtensionView extensionView = new CatalogExtensionView(
                    extension,
                    defaultGroup,
                    () -> configChanged(extension),
                    () -> propertiesChanged(extension),
                    a -> aliasChanged(extension, a),
                    c -> defaultCatalogChanged(c),
                    e -> enabledChanged(extension, e));
            catalogExtensionViews.add(extensionView);
            boolean last = y == config.getCatalogs().size() - 1;
            view.getPanelCatalogs().add(extensionView, new GridBagConstraints(0, y++, 1, 1, 1, last ? 1 : 0, GridBagConstraints.BASELINE, GridBagConstraints.HORIZONTAL, insets, 0, 0));
        }

        view.getEditorsTabbedPane().addChangeListener(new SelectedFileListener());
        view.setExitAction(this::exit);

        view.getMemoryLabel().setText(getMemoryString());
        new Timer(250, evt -> view.getMemoryLabel().setText(getMemoryString())).start();
    }

    private void exit()
    {
        List<QueryFileModel> dirtyFiles = model
                .getFiles()
                .stream()
                .filter(f -> f.isDirty())
                .collect(toList());
        if (dirtyFiles.size() == 0)
        {
            System.exit(0);
        }

        int result = JOptionPane.showConfirmDialog(
                view,
                "Save changes to the following files: " +
                    System.lineSeparator() +
                    dirtyFiles
                            .stream()
                            .map(f -> FilenameUtils.getName(f.getFilename()))
                            .collect(joining(System.lineSeparator())),
                "Unsaved changes",
                JOptionPane.YES_NO_CANCEL_OPTION);

        if (result == JOptionPane.CANCEL_OPTION)
        {
            return;
        }
        else if (result == JOptionPane.NO_OPTION)
        {
            System.exit(0);
        }

        for (QueryFileModel file : dirtyFiles)
        {
            // Abort on first Cancel
            if (!save(file))
            {
                return;
            }
        }

        System.exit(0);
    }

    private void configChanged(ICatalogExtension catalogExtension)
    {
        for (Config.Catalog catalog : config.getCatalogs())
        {
            if (catalog.getCatalogExtension() == catalogExtension)
            {
                catalog.setConfig(catalogExtension.getProperties());
            }
        }

        saveConfig();
        // Also populate new properties in current session if config changed
        propertiesChanged(catalogExtension);
    }

    /** Properties of a catalog extension changed. Setup current query file with it's new properties */
    private void propertiesChanged(ICatalogExtension catalogExtension)
    {
        QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
        if (editor != null)
        {
            CatalogExtensionModel model = editor.getFile().getCatalogExtensions().get(catalogExtension);
            catalogExtension.setup(model.getAlias(), editor.getFile().getQuerySession());
        }
    }

    private void aliasChanged(ICatalogExtension catalogExtension, String alias)
    {
        QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
        if (editor != null)
        {
            CatalogExtensionModel model = editor.getFile().getCatalogExtensions().get(catalogExtension);
            model.setAlias(alias);
        }
    }

    private void enabledChanged(ICatalogExtension catalogExtension, boolean enabled)
    {
        QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
        if (editor != null)
        {
            CatalogExtensionModel model = editor.getFile().getCatalogExtensions().get(catalogExtension);
            model.setEnabled(enabled);
        }
    }

    private void defaultCatalogChanged(String catalogAlias)
    {
        QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
        if (editor != null)
        {
            editor.getFile().getQuerySession().setDefaultCatalog(catalogAlias);
        }
    }

    private String getMemoryString()
    {
        Runtime runtime = Runtime.getRuntime();
        return String.format("%s / %s", byteCountToDisplaySize(runtime.totalMemory()), byteCountToDisplaySize(runtime.freeMemory()));
    }

    private void saveConfig()
    {
        Main.saveConfig(config);
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
                defaultGroup.clearSelection();
                catalogExtensionViews.forEach(v -> v.init(editor.getFile()));
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

            QueryFileModel fileModel = editor.getFile();
            if (fileModel.getState() == State.EXECUTING)
            {
                return;
            }

            // Setup catalogs
            fileModel.getQuerySession().getCatalogRegistry().clearCatalogs();
            fileModel.getCatalogExtensions().entrySet().forEach(e ->
            {
                ICatalogExtension extension = e.getKey();
                CatalogExtensionModel model = e.getValue();
                if (model.isEnabled())
                {
                    // Register catalog in session
                    fileModel.getQuerySession().getCatalogRegistry().registerCatalog(model.getAlias(), extension.getCatalog());
                    // Setup extensions from model data
                    // Do this here also besides when properties changes,
                    // since there is a possibility that no changes was made before a query is executed.
                    extension.setup(model.getAlias(), fileModel.getQuerySession());
                }
            });

            PayloadbuilderService.executeQuery(fileModel, queryString, () ->
            {
                // Update properties in UI after query is finished
                // Change current index/instance/database etc. that was altered in query
                catalogExtensionViews.forEach(v -> v.init(fileModel));
            });
        }
    }
    
    private class EditVariablesListener implements Runnable
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

            QueryFileModel fileModel = editor.getFile();

            Set<String> variableNames = PayloadbuilderService.getVariables(queryString);

            for (String name : variableNames)
            {
                if (!fileModel.getVariables().containsKey(name))
                {
                    fileModel.getVariables().put(name, null);
                }
            }

            variablesDialog.init(FilenameUtils.getName(fileModel.getFilename()), fileModel.getVariables());
            variablesDialog.setVisible(true);

            Map<String, Object> variables = variablesDialog.getVariables();
            if (variables != null)
            {
                fileModel.getVariables().clear();
                fileModel.getVariables().putAll(variables);
            }
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
            QueryFileModel queryFile = new QueryFileModel(config.getCatalogs());
            queryFile.setFilename("Query" + (newFileCounter++) + ".sql");
            model.addFile(queryFile);
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
                QueryFileModel file = new QueryFileModel(config.getCatalogs(), fileChooser.getSelectedFile());
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
