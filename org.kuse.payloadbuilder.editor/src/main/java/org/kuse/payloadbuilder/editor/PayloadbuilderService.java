package org.kuse.payloadbuilder.editor;

import static java.util.Arrays.asList;
import static java.util.Collections.emptySet;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.Stack;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.Payloadbuilder;
import org.kuse.payloadbuilder.core.QueryResult;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;
import org.kuse.payloadbuilder.core.parser.AExpressionVisitor;
import org.kuse.payloadbuilder.core.parser.AStatementVisitor;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.ParseException;
import org.kuse.payloadbuilder.core.parser.QueryParser;
import org.kuse.payloadbuilder.core.parser.QueryStatement;
import org.kuse.payloadbuilder.core.parser.VariableExpression;
import org.kuse.payloadbuilder.editor.QueryFileModel.Output;
import org.kuse.payloadbuilder.editor.QueryFileModel.State;

/** Class the executes queries etc. */
class PayloadbuilderService
{
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
    static void executeQuery(
            QueryFileModel file,
            String queryString,
            Runnable queryFinnishedCallback)
    {
        EXECUTOR.execute(() ->
        {
            int queryId = file.incrementAndGetQueryId();
            file.setState(State.EXECUTING);
            try
            {
                file.getQuerySession().setAbortSupplier(() -> file.getState() == State.ABORTED);
                QueryResult queryResult = Payloadbuilder.query(file.getQuerySession(), queryString);

                while (queryResult.hasMoreResults())
                {
                    if (file.getState() == State.ABORTED)
                    {
                        break;
                    }
                    ResultModel resultModel = new ResultModel(file);
                    file.addResult(resultModel);
                    OutputWriter writer = null;
                    if (file.getOutput() == Output.TABLE)
                    {
                        writer = new ObjectWriter(resultModel);
                    }
                    else
                    {
                        writer = new NoneOutputWriter(resultModel);
                    }
                    queryResult.writeResult(writer);

                    resultModel.done();
                    file
                            .getQuerySession()
                            .printLine(
                                    String.valueOf(resultModel.getActualRowCount())
                                        + " row(s) selected"
                                        + System.lineSeparator());
                }

                if (file.getState() == State.EXECUTING)
                {
                    file.setState(State.COMPLETED);
                }
            }
            catch (ParseException e)
            {
                file.setError(String.format("Syntax error. Line: %d, Column: %d. %s", e.getLine(), e.getColumn(), e.getMessage()));
                file.setParseErrorLocation(Pair.of(e.getLine(), e.getColumn()));
                file.setState(State.ERROR);
            }
            catch (Exception e)
            {
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
            }
            finally
            {
                if (queryId == file.getQueryId())
                {
                    if (file.getState() == State.EXECUTING)
                    {
                        file.setState(State.COMPLETED);
                    }
                    queryFinnishedCallback.run();
                }
            }
        });
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
        private final ResultModel resultModel;

        NoneOutputWriter(ResultModel resultModel)
        {
            this.resultModel = resultModel;
        }

        @Override
        public void endRow()
        {
            resultModel.addRow(ArrayUtils.EMPTY_OBJECT_ARRAY);
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
    private static class ObjectWriter implements OutputWriter
    {
        private final Stack<Object> parent = new Stack<>();
        private final Stack<String> currentField = new Stack<>();
        private final ResultModel resultModel;

        ObjectWriter(ResultModel resultModel)
        {
            this.resultModel = resultModel;
        }

        /** Returns written value and clears state */
        private Object[] getValue(int rowNumber)
        {
            currentField.clear();
            Object v = parent.pop();
            if (!(v instanceof PairList))
            {
                throw new RuntimeException("Expected a list of string/value pairs but got " + v);
            }

            PairList pairList = (PairList) v;

            Object[] result = new Object[pairList.size() + 1];
            int index = 0;
            result[index++] = rowNumber;
            for (Pair<String, Object> pair : pairList)
            {
                result[index++] = pair.getValue();
            }

            return result;
        }

        @Override
        public void initResult(String[] columns)
        {
            if (columns != null)
            {
                List<String> columnList = new ArrayList<>(asList(columns));
                // Row id column
                columnList.add(0, "");
                resultModel.setColumns(columns);
            }
        }

        @Override
        public void endRow()
        {
            // Adjust columns
            PairList pairList = (PairList) parent.peek();
            if (resultModel.getColumnCount() < (pairList.size() + 1))
            {
                List<String> columns = new ArrayList<>(pairList.size() + 1);
                // Row id column
                columns.add("");
                pairList.stream().map(p -> p.getKey()).forEach(column -> columns.add(column));
                resultModel.setColumns(columns.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
            }

            resultModel.addRow(getValue(resultModel.getRowCount() + 1));
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

            putValue(value);
        }

        @Override
        public void startObject()
        {
            // Root object should not be a map
            // since we might have duplicate column names
            if (parent.size() == 0)
            {
                parent.push(new PairList());
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
        private static class PairList extends ArrayList<Pair<String, Object>>
        {
        }
    }
}
