package com.viskan.payloadbuilder.editor;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.analyze.OperatorBuilder;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.editor.QueryFile.Output;
import com.viskan.payloadbuilder.operator.JsonStringWriter;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.parser.ParseException;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.Query;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.io.FileUtils.byteCountToDisplaySize;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.beans.PropertyChangeEvent;
import java.beans.PropertyChangeListener;
import java.io.File;
import java.util.Iterator;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;
import javax.swing.Timer;
import javax.swing.event.ChangeEvent;
import javax.swing.event.ChangeListener;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;

/** Main controller for editor */
public class PayloadbuilderEditorController implements PropertyChangeListener
{
    private static final Executor EXECUTOR = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);
    private final PayloadbuilderEditorView view;
    private final PayloadbuilderEditorModel model;
    private final CaretChangedListener caretChangedListener = new CaretChangedListener();
    private int newFileCounter = 1;

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
        view.setCancelAction(() -> 
        {
            QueryEditorContentView editor = (QueryEditorContentView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                editor.getFile().setExecuting(false);
            }
        });
        view.setNewQueryAction(new NewQueryListener());
        //        view.setFormatAction(new FormatListener());
        view.setSaveAction(new SaveListener());
        view.setOpenAction(new OpenListener());
        view.setToogleResultAction(() ->
        {
            QueryEditorContentView editor = (QueryEditorContentView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                editor.toggleResultPane();
            }
        });
        view.setToggleCommentRunnable(() ->
        {
            QueryEditorContentView editor = (QueryEditorContentView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                editor.toggleComments();
            }
        });
        view.setOutputChangedAction(() ->
        {
            QueryEditorContentView editor = (QueryEditorContentView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                editor.getFile().setOutput((Output) view.getOutputCombo().getSelectedItem());
            }
        });

        view.getEditorsTabbedPane().addChangeListener(new SelectFileListener());
        view.setExitAction(() -> System.exit(0));

        view.getMemoryLabel().setText(getMemoryString());
        new Timer(250, evt -> view.getMemoryLabel().setText(getMemoryString())).start();
    }

    private String getMemoryString()
    {
        Runtime runtime = Runtime.getRuntime();
        return String.format("%s / %s", byteCountToDisplaySize(runtime.totalMemory()), byteCountToDisplaySize(runtime.freeMemory()));
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
                QueryEditorContentView content = new QueryEditorContentView(
                        file,
                        text -> file.setQuery(text),
                        caretChangedListener);
                view.getEditorsTabbedPane().add(content);
                view.getEditorsTabbedPane().setTabComponentAt(model.getFiles().size() - 1, new TabComponentView(file, () ->
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
                }));
                view.getEditorsTabbedPane().setSelectedIndex(model.getFiles().size() - 1);
                content.requestFocusInWindow();
            }
            // Set selected
            else
            {
            }
        }
    };

    private class SelectFileListener implements ChangeListener
    {
        @Override
        public void stateChanged(ChangeEvent e)
        {
            int index = view.getEditorsTabbedPane().getSelectedIndex();
            if (index >= 0)
            {
                QueryEditorContentView editor = (QueryEditorContentView) view.getEditorsTabbedPane().getSelectedComponent();
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
            QueryEditorContentView editor = (QueryEditorContentView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor == null)
            {
                return;
            }
            String queryString = editor.getQuery(true);
            if (isBlank(queryString))
            {
                return;
            }
            EXECUTOR.execute(() ->
            {
                // TODO: move this to another class
                QueryFile file = editor.getFile();
                editor.startQuery();
                StopWatch sw = new StopWatch();
                sw.start();
                file.setExecuting(true);
                int rowCount = 0;
                boolean canceled = false;
                try
                {
                    CatalogRegistry reg = new CatalogRegistry();
                    QueryParser parser = new QueryParser();
                    Query query = parser.parseQuery(reg, queryString);

                    Pair<Operator, Projection> pair = OperatorBuilder.create(reg, query);
                    Operator op = pair.getKey();
                    Projection pr = pair.getValue();
                    JsonStringWriter jsw = new JsonStringWriter();

                    StringBuilder sb = new StringBuilder();
                    OperatorContext context = new OperatorContext();
                    if (op != null)
                    {
                        Iterator<Row> it = op.open(context);
                        while (it.hasNext())
                        {
                            // Check if model says we should break execution
                            if (!file.isExecuting())
                            {
                                canceled = true;
                                break;
                            }
                            
                            Row row = it.next();
                            if (file.getOutput() == Output.JSON_RAW)
                            {
                                pr.writeValue(jsw, context, row);
                                sb.append(jsw.getAndReset());
                                sb.append(System.lineSeparator());
                            }
                            rowCount++;
                            editor.setRowCount(rowCount);
                            editor.setRunTime(sw.getTime(TimeUnit.MILLISECONDS));
                        }
                    }
                    else
                    {
                        TableAlias alias = TableAlias.of(null, QualifiedName.of("table"), "t");
                        Row row = Row.of(alias, 0, ArrayUtils.EMPTY_OBJECT_ARRAY);
                        if (file.getOutput() == Output.JSON_RAW)
                        {
                            pr.writeValue(jsw, context, row);
                            sb.append(jsw.getAndReset());
                        }
                        rowCount++;
                    }

                    editor.setMessage(sb.toString());
                    editor.clearHighLights();
                }
                catch (ParseException e)
                {
                    editor.highLight(e.getLine(), e.getColumn());
                    editor.setMessage(String.format("Syntax error. Line: %d, Column: %d. %s", e.getLine(), e.getColumn(), e.getMessage()));
                }
                catch (Exception e)
                {
                    editor.setMessage("Error. " + e.getMessage());
                    e.printStackTrace();
                }
                finally
                {
                    sw.stop();
                    file.setExecuting(false);
                    editor.stopQuery();
                    editor.setRowCount(rowCount);
                    editor.setRunTime(sw.getTime(TimeUnit.MILLISECONDS));
                    if (canceled)
                    {
                        editor.setMessage("Aborted!");
                    }
                }
            });
        }
    }

    private class CaretChangedListener implements Consumer<QueryEditorContentView>
    {
        @Override
        public void accept(QueryEditorContentView t)
        {
            view.getCaretLabel().setText(String.format("%d : %d : %d", t.getCaretLineNumber(), t.getCaretOffsetFromLineStart(), t.getCaretPosition()));
        }
    }

    private class NewQueryListener implements Runnable
    {
        @Override
        public void run()
        {
            QueryFile queryFile = new QueryFile();
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
            QueryEditorContentView editor = (QueryEditorContentView) view.getEditorsTabbedPane().getSelectedComponent();
            if (editor != null)
            {
                QueryFile file = editor.getFile();
                save(file);
            }
        }
    }
}
