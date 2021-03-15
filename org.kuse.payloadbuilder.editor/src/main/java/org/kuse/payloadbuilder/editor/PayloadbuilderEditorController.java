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
import java.util.Objects;
import java.util.Set;
import java.util.function.Consumer;

import javax.swing.ButtonGroup;
import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.Timer;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.apache.commons.io.FilenameUtils;
import org.kuse.payloadbuilder.editor.QueryFileModel.Format;
import org.kuse.payloadbuilder.editor.QueryFileModel.Output;
import org.kuse.payloadbuilder.editor.QueryFileModel.State;

import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;

/** Main controller for editor */
class PayloadbuilderEditorController implements PropertyChangeListener
{
    private static final int TIMER_INTERVAL = 250;
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

    //CSOFF
    private void init()
    //CSON
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

        NewQueryListener newQueryListener = new NewQueryListener();
        view.setNewQueryAction(newQueryListener);
        view.setSaveAction(new SaveListener());
        view.setSaveAsAction(new SaveAsListener());
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
        view.setFormatChangedAction(() ->
        {
            QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                editor.getFile().setFormat((Format) view.getFormatCombo().getSelectedItem());
            }
        });
        view.setOpenRecentFileConsumer(file ->
        {
            int length = model.getFiles().size();
            for (int i = 0; i < length; i++)
            {
                if (model.getFiles().get(i).getFilename().equals(file))
                {
                    model.setSelectedFile(i);
                    // TODO: remove this when proper binding exist in PayloadBuilderEditorModel
                    //       for selected file
                    view.getEditorsTabbedPane().setSelectedIndex(i);
                    return;
                }
            }

            QueryFileModel queryFile = new QueryFileModel(config.getCatalogs(), new File(file));
            model.addFile(queryFile);
            config.appendRecentFile(file);
            saveConfig();
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
        new Timer(TIMER_INTERVAL, evt -> view.getMemoryLabel().setText(getMemoryString())).start();
        newQueryListener.run();
        view.setRecentFiles(config.getRecentFiles());
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
                "Save changes to the following files: "
                    + System.lineSeparator()
                    + dirtyFiles
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
            if (!save(file, false))
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
            editor.getFile().getQuerySession().getCatalogRegistry().setDefaultCatalog(catalogAlias);
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
        view.setRecentFiles(config.getRecentFiles());
    }

    private boolean save(QueryFileModel file, boolean saveAs)
    {
        if (!saveAs && !file.isDirty())
        {
            return true;
        }

        if (saveAs || file.isNew())
        {
            //CSOFF
            JFileChooser fileChooser = new JFileChooser()
            //CSON
            {
                @Override
                public void approveSelection()
                {
                    File f = getSelectedFile();
                    if (f.exists() && getDialogType() == SAVE_DIALOG)
                    {
                        int result = JOptionPane.showConfirmDialog(this, "The file exists, overwrite?", "Existing file", JOptionPane.YES_NO_CANCEL_OPTION);
                        //CSOFF
                        switch (result)
                        //CSON
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
        config.appendRecentFile(file.getFilename());
        saveConfig();
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
                final QueryFileView content = new QueryFileView(
                        file,
                        text -> file.setQuery(text),
                        caretChangedListener);
                view.getEditorsTabbedPane().add(content);

                final TabComponentView tabComponent = new TabComponentView(file.getTabTitle(), null, () ->
                {
                    //CSOFF
                    if (file.isDirty())
                    //CSON
                    {
                        int result = JOptionPane.showConfirmDialog(view, "Save changes ?", "Save", JOptionPane.YES_NO_CANCEL_OPTION);
                        //CSOFF
                        if (result == JOptionPane.CANCEL_OPTION)
                        //CSON
                        {
                            return;
                        }
                        else if (result == JOptionPane.YES_OPTION)
                        {
                            //CSOFF
                            if (!save(file, false))
                            //CSON
                            {
                                return;
                            }
                        }
                    }

                    // Find tab index with "this" content
                    int length = view.getEditorsTabbedPane().getComponents().length;
                    for (int i = 0; i < length; i++)
                    {
                        //CSOFF
                        if (view.getEditorsTabbedPane().getComponents()[i] == content)
                        //CSON
                        {
                            model.removeFile(file);
                            view.getEditorsTabbedPane().remove(content);
                            break;
                        }
                    }
                });

                // Set title and tooltip upon change
                file.addPropertyChangeListener(l ->
                {
                    tabComponent.setTitle(file.getTabTitle());
                    int length = view.getEditorsTabbedPane().getTabCount();
                    for (int i = 0; i < length; i++)
                    {
                        //CSOFF
                        if (view.getEditorsTabbedPane().getTabComponentAt(i) == tabComponent)
                        //CSON
                        {
                            view.getEditorsTabbedPane().setToolTipTextAt(i, file.getFilename());
                            break;
                        }
                    }
                });

                int index = model.getFiles().size() - 1;
                view.getEditorsTabbedPane().setToolTipTextAt(index, file.getFilename());
                view.getEditorsTabbedPane().setTabComponentAt(index, tabComponent);
                view.getEditorsTabbedPane().setSelectedIndex(index);
                content.requestFocusInWindow();
            }
        }
    };

    /** Selected file listener */
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
                view.getFormatCombo().setSelectedItem(editor.getFile().getFormat());
                defaultGroup.clearSelection();
                catalogExtensionViews.forEach(v -> v.init(editor.getFile()));
            }
        }
    }

    /** Execute listener */
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
                QueryFileView current = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();

                // Only update extensions panel if the completed query is current query file
                if (fileModel == current.getFile())
                {
                    // Update properties in UI after query is finished
                    // Change current index/instance/database etc. that was altered in query
                    catalogExtensionViews.forEach(v -> v.init(fileModel));
                }
            });
        }
    }

    /** Edit vars listener */
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

            Set<String> variableNames = PayloadbuilderService.getVariables(fileModel.getQuerySession().getCatalogRegistry(), queryString);

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

    /** Caret change listener */
    private class CaretChangedListener implements Consumer<QueryFileView>
    {
        @Override
        public void accept(QueryFileView t)
        {
            view.getCaretLabel().setText(String.format("%d : %d : %d", t.getCaretLineNumber(), t.getCaretOffsetFromLineStart(), t.getCaretPosition()));
        }
    }

    /** New query listener */
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

    /** Open listener */
    private class OpenListener implements Runnable
    {
        @Override
        public void run()
        {
            JFileChooser fileChooser = new JFileChooser();
            fileChooser.setFileSelectionMode(JFileChooser.FILES_ONLY);
            fileChooser.setMultiSelectionEnabled(true);
            if (!isBlank(config.getLastOpenPath()))
            {
                fileChooser.setCurrentDirectory(new File(config.getLastOpenPath()));
            }
            if (fileChooser.showOpenDialog(view) == JFileChooser.APPROVE_OPTION)
            {
                if (fileChooser.getSelectedFiles().length <= 0)
                {
                    return;
                }

                for (File selectedFile : fileChooser.getSelectedFiles())
                {
                    QueryFileModel file = new QueryFileModel(config.getCatalogs(), selectedFile);
                    model.addFile(file);
                    config.appendRecentFile(selectedFile.getAbsolutePath());
                }

                // Store last selected path if differs
                if (!Objects.equals(config.getLastOpenPath(), fileChooser.getCurrentDirectory().getAbsolutePath()))
                {
                    config.setLastOpenPath(fileChooser.getCurrentDirectory().getAbsolutePath());
                }
                saveConfig();
            }
        }
    }

    /** Save listener */
    private class SaveListener implements Runnable
    {
        @Override
        public void run()
        {
            QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                QueryFileModel file = editor.getFile();
                save(file, false);
            }
        }
    }

    /** Save As listener */
    private class SaveAsListener implements Runnable
    {
        @Override
        public void run()
        {
            QueryFileView editor = (QueryFileView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                QueryFileModel file = editor.getFile();
                save(file, true);
            }
        }
    }
}
