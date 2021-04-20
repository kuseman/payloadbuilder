package org.kuse.payloadbuilder.editor;

import static java.util.Collections.emptySet;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.io.Reader;
import java.io.Writer;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import javax.swing.JFileChooser;
import javax.swing.JOptionPane;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.io.output.CountingOutputStream;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.CompiledQuery;
import org.kuse.payloadbuilder.core.CsvOutputWriter;
import org.kuse.payloadbuilder.core.JsonOutputWriter;
import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.Payloadbuilder;
import org.kuse.payloadbuilder.core.QueryResult;
import org.kuse.payloadbuilder.core.catalog.CatalogException;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.parser.AExpressionVisitor;
import org.kuse.payloadbuilder.core.parser.AStatementVisitor;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ParseException;
import org.kuse.payloadbuilder.core.parser.QueryParser;
import org.kuse.payloadbuilder.core.parser.QueryStatement;
import org.kuse.payloadbuilder.core.parser.VariableExpression;
import org.kuse.payloadbuilder.editor.ICatalogExtension.ExceptionAction;
import org.kuse.payloadbuilder.editor.QueryFileModel.Format;
import org.kuse.payloadbuilder.editor.QueryFileModel.Output;
import org.kuse.payloadbuilder.editor.QueryFileModel.State;

/** Class the executes queries etc. */
class PayloadbuilderService
{
    private static final Runnable NO_OP = () ->
    {
    };
    private static final int FILE_WRITER_BUFFER_SIZE = 4096;
    private static final VariableVisitor VARIABLES_VISITOR = new VariableVisitor();
    private static final QueryParser PARSER = new QueryParser();
    private static final AtomicInteger THREAD_ID = new AtomicInteger(1);
    private static final Executor EXECUTOR = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2, r ->
    {
        Thread thread = new Thread(r);
        thread.setDaemon(true);
        thread.setName("QueryExecutor-#" + THREAD_ID.getAndIncrement());
        thread.setPriority(Thread.MIN_PRIORITY);
        return thread;
    });

    /** Execute query for provider query file */
    //CSOFF
    static void executeQuery(
            //CSON
            QueryFileModel file,
            String queryString,
            Runnable queryFinnishedCallback)
    {
        MutableObject<String> outputFileName = new MutableObject<>();
        if (file.getOutput() == Output.FILE)
        {
            String fileName = getOutputFileName();
            // Abort
            if (fileName == null)
            {
                return;
            }
            outputFileName.setValue(fileName);
        }

        EXECUTOR.execute(() ->
        {
            boolean completed = false;
            while (!completed)
            {
                int queryId = file.incrementAndGetQueryId();
                file.setState(State.EXECUTING);
                EditorOutputWriter writer = null;
                try
                {
                    file.getQuerySession().setAbortSupplier(() -> file.getState() == State.ABORTED);
                    CompiledQuery query = Payloadbuilder.compile(queryString, file.getQuerySession().getCatalogRegistry());
                    QueryResult queryResult = query.execute(file.getQuerySession());

                    writer = getOutputWriter(file, outputFileName.getValue());

                    while (queryResult.hasMoreResults())
                    {
                        writer.newResultSet.run();
                        if (file.getState() == State.ABORTED)
                        {
                            break;
                        }

                        queryResult.writeResult(writer.writer);
                        // Flush after each result set
                        writer.writer.flush();
                        writer.resultSetComplete.run();
                    }

                    writer.complete.run();
                    if (file.getState() == State.EXECUTING)
                    {
                        file.setState(State.COMPLETED);
                    }

                    completed = true;
                }
                catch (ParseException e)
                {
                    file.setError(String.format("Syntax error. Line: %d, Column: %d. %s", e.getLine(), e.getColumn(), e.getMessage()));
                    file.setParseErrorLocation(Pair.of(e.getLine(), e.getColumn()));
                    file.setState(State.ERROR);
                    completed = true;
                }
                catch (Exception e)
                {
                    if (e instanceof CatalogException)
                    {
                        CatalogException ce = (CatalogException) e;
                        Optional<ICatalogExtension> catalogExtension = file.getCatalogExtensions()
                                .entrySet()
                                .stream()
                                .filter(kv -> Objects.equals(ce.getCatalogAlias(), kv.getValue().getAlias()))
                                .map(kv -> kv.getKey())
                                .findFirst();

                        if (catalogExtension.isPresent()
                            && catalogExtension.get().handleException(file.getQuerySession(), ce) == ExceptionAction.RERUN)
                        {
                            // Re-run query
                            continue;
                        }
                    }

                    // Only set error messages if this is the latest query made
                    if (queryId == file.getQueryId())
                    {
                        String message = e.getMessage();
                        if (e.getCause() != null)
                        {
                            message += " (" + e.getCause().getClass().getSimpleName() + ")";
                        }
                        file.setError(message);
                        file.setState(State.ERROR);
                        if (System.getProperty("devEnv") != null)
                        {
                            e.printStackTrace();
                        }
                    }

                    completed = true;
                }
                finally
                {
                    if (writer != null)
                    {
                        writer.writer.close();
                    }

                    if (queryId == file.getQueryId())
                    {
                        if (file.getState() == State.EXECUTING)
                        {
                            file.setState(State.COMPLETED);
                        }
                        queryFinnishedCallback.run();
                    }
                }
            }
        });
    }

    /** Extened interface for editor output writers */
    private static class EditorOutputWriter
    {
        private final OutputWriter writer;
        private final Runnable newResultSet;
        private final Runnable resultSetComplete;
        private final Runnable complete;

        EditorOutputWriter(OutputWriter writer, Runnable newResultSet, Runnable resultSetComplete, Runnable complete)
        {
            this.writer = writer;
            this.newResultSet = newResultSet;
            this.resultSetComplete = resultSetComplete;
            this.complete = complete;
        }
    }

    private static EditorOutputWriter getOutputWriter(QueryFileModel file, String outputFileName)
    {
        if (file.getOutput() == Output.TABLE)
        {
            final TableOutputWriter writer = new TableOutputWriter();
            return new EditorOutputWriter(writer, () ->
            {
                // Create new model
                ResultModel model = new ResultModel(file);
                writer.setResultModel(model);
                file.addResult(model);
            }, () ->
            {
                // Mark current model as done and print result
                writer.resultModel.done();
                printRowCountResult(file, writer.resultModel.getActualRowCount());
            }, NO_OP);
        }
        else if (file.getOutput() == Output.NONE)
        {
            final NoneOutputWriter writer = new NoneOutputWriter(file);
            return new EditorOutputWriter(writer, () -> writer.rowCount = 0, () -> printRowCountResult(file, writer.rowCount), () ->
            {
            });
        }
        else if (file.getOutput() == Output.TEXT
            || file.getOutput() == Output.FILE)
        {
            Writer output = file.getQuerySession().getPrintWriter();
            final MutableObject<CountingOutputStream> countingOutputStream = new MutableObject<>();
            final MutableObject<OutputWriter> writer = new MutableObject<>();

            // Redirect output to chosen file
            if (file.getOutput() == Output.FILE)
            {
                try
                {
                    countingOutputStream.setValue(new CountingOutputStream(
                            new FileOutputStream(
                                    new File(outputFileName))));

                    output = new BufferedWriter(
                            new OutputStreamWriter(countingOutputStream.getValue(), StandardCharsets.UTF_8), FILE_WRITER_BUFFER_SIZE);
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Error creating writer for output " + outputFileName);
                }
            }

            if (file.getFormat() == Format.CSV)
            {
                writer.setValue(new CsvOutputWriter(output)
                {
                    @Override
                    public void endRow()
                    {
                        super.endRow();
                        file.incrementTotalRowCount();
                    };
                });
            }
            else if (file.getFormat() == Format.JSON)
            {
                writer.setValue(new JsonOutputWriter(output)
                {
                    @Override
                    public void endRow()
                    {
                        super.endRow();
                        file.incrementTotalRowCount();
                    };
                });
            }

            if (writer.getValue() == null)
            {
                throw new RuntimeException("Unsupported format " + file.getFormat());
            }

            return new EditorOutputWriter(writer.getValue(), NO_OP,
                    () ->
                    {
                        //CSOFF
                        if (countingOutputStream.getValue() == null)
                        //CSON
                        {
                            file.getQuerySession().printLine("");
                        }
                    },
                    () ->
                    {
                        //CSOFF
                        if (countingOutputStream.getValue() != null)
                        //CSON
                        {
                            printBytesWritten(file, countingOutputStream.getValue().getByteCount(), outputFileName);
                        }
                    });
        }

        throw new RuntimeException("Unsupported output " + file.getOutput());
    }

    private static String getOutputFileName()
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
        fileChooser.setDialogTitle("Select output filename");
        int result = fileChooser.showSaveDialog(null);
        if (result == JFileChooser.APPROVE_OPTION)
        {
            return fileChooser.getSelectedFile().getAbsolutePath();
        }

        return null;
    }

    private static void printRowCountResult(QueryFileModel file, int rowCount)
    {
        file
                .getQuerySession()
                .printLine(
                        String.valueOf(rowCount)
                            + " row(s) selected"
                            + System.lineSeparator());
    }

    private static void printBytesWritten(QueryFileModel file, long bytes, String filename)
    {
        file
                .getQuerySession()
                .printLine(
                        FileUtils.byteCountToDisplaySize(bytes)
                            + " written to "
                            + filename
                            + System.lineSeparator());
    }

    /** Get named parameters from query */
    static Set<String> getVariables(CatalogRegistry registry, String query)
    {
        QueryStatement parsedQuery;
        try
        {
            parsedQuery = PARSER.parseQuery(registry, query);
        }
        catch (Exception e)
        {
            // TODO: notify error parsing
            return emptySet();
        }
        Set<String> parameters = new HashSet<>();
        parsedQuery.getStatements().forEach(s -> s.accept(VARIABLES_VISITOR, parameters));
        return parameters;
    }

    /** Variable visitor */
    private static class VariableVisitor extends AStatementVisitor<Void, Set<String>>
    {
        private static final ExpressionVisitor EXPRESSION_VISITOR = new ExpressionVisitor();

        @Override
        protected void visitExpression(Set<String> context, Expression expression)
        {
            expression.accept(EXPRESSION_VISITOR, context);
        }

        /** Expression visitor */
        private static class ExpressionVisitor extends AExpressionVisitor<Void, Set<String>>
        {
            @Override
            public Void visit(VariableExpression expression, Set<String> context)
            {
                context.add(expression.getName());
                return null;
            }
        }
    }

    /** Output writer used in NONE output mode */
    private static class NoneOutputWriter implements OutputWriter
    {
        private final QueryFileModel file;
        private int rowCount;

        NoneOutputWriter(QueryFileModel file)
        {
            this.file = file;
        }

        @Override
        public void endRow()
        {
            file.incrementTotalRowCount();
            rowCount++;
        }

        @Override
        public void writeFieldName(String name)
        {
        }

        @Override
        public void writeValue(Object value)
        {
        }

        @Override
        public void startObject()
        {
        }

        @Override
        public void endObject()
        {
        }

        @Override
        public void startArray()
        {
        }

        @Override
        public void endArray()
        {
        }
    }

    /** Writer that writes object structure from a projection */
    static class TableOutputWriter implements OutputWriter
    {
        private ResultModel resultModel;

        private final Stack<Object> parent = new Stack<>();
        private final Stack<String> currentField = new Stack<>();

        void setResultModel(ResultModel resultModel)
        {
            this.resultModel = resultModel;
        }

        /** Returns written value and clears state */
        private PairList getValue(int rowNumber)
        {
            currentField.clear();
            Object v = parent.pop();
            if (!(v instanceof PairList))
            {
                throw new RuntimeException("Expected a list of string/value pairs but got " + v);
            }

            PairList result = (PairList) v;
            result.add(0, Pair.of("", rowNumber));
            return (PairList) v;
        }

        @Override
        public void initResult(String[] columns)
        {
            resultModel.setColumns(columns);
        }

        @Override
        public void endRow()
        {
            // Adjust columns
            PairList pairList = getValue(resultModel.getRowCount() + 1);
            if (resultModel.getColumnCount() < pairList.size())
            {
                resultModel.setColumns(pairList.columns.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
            }

            resultModel.addRow(pairList);
        }

        @Override
        public void writeFieldName(String name)
        {
            currentField.push(name);
        }

        @Override
        public void writeValue(Object input)
        {
            Object value = input;
            if (value instanceof Iterator)
            {
                @SuppressWarnings("unchecked")
                Iterator<Object> it = (Iterator<Object>) value;
                startArray();
                while (it.hasNext())
                {
                    writeValue(it.next());
                }
                endArray();
                return;
            }
            else if (value instanceof Reader)
            {
                try (Reader reader = (Reader) value)
                {
                    value = IOUtils.toString(reader);
                }
                catch (IOException e)
                {
                    throw new RuntimeException("Error reading reader to string", e);
                }
            }

            putValue(value);
        }

        @Override
        public void startObject()
        {
            // Root object should not be a map
            // since we might have duplicate column names
            if (parent.size() == 0)
            {
                //CSOFF
                parent.push(new PairList(10));
                //CSON
            }
            else
            {
                parent.push(new LinkedHashMap<>());
            }
        }

        @Override
        public void endObject()
        {
            putValue(parent.pop());
        }

        @Override
        public void startArray()
        {
            parent.push(new ArrayList<>());
        }

        @Override
        public void endArray()
        {
            putValue(parent.pop());
        }

        @SuppressWarnings("unchecked")
        private void putValue(Object value)
        {
            // Top of stack put value back
            if (parent.isEmpty())
            {
                parent.push(value);
                return;
            }

            Object p = parent.peek();

            if (p instanceof PairList)
            {
                ((PairList) p).add(Pair.of(currentField.pop(), value));
            }
            else if (p instanceof Map)
            {
                ((Map<String, Object>) p).put(currentField.pop(), value);
            }
            else if (p instanceof List)
            {
                ((List<Object>) p).add(value);
            }
        }

        /** Pair list */
        static class PairList extends ArrayList<Pair<String, Object>>
        {
            static final PairList EMPTY = new PairList(0);
            private final List<String> columns;

            private PairList(int capacity)
            {
                super(capacity);
                columns = new ArrayList<>(capacity);
            }

            List<String> getColumns()
            {
                return columns;
            }

            @Override
            public void add(int index, Pair<String, Object> pair)
            {
                columns.add(index, pair.getKey());
                super.add(index, pair);
            }

            @Override
            public boolean add(Pair<String, Object> pair)
            {
                columns.add(pair.getKey());
                return super.add(pair);
            }
        }
    }
}
