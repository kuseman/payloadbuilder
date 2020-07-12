package com.viskan.payloadbuilder.editor;

import com.viskan.payloadbuilder.OutputWriter;
import com.viskan.payloadbuilder.Payloadbuilder;
import com.viskan.payloadbuilder.QueryResult;
import com.viskan.payloadbuilder.editor.QueryFileModel.State;
import com.viskan.payloadbuilder.parser.ParseException;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

/** Class the executes queries etc. */
class PayloadbuilderService
{
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
                    setupColumns(resultModel, queryResult.getResultMetaData());

                    ObjectWriter writer = new ObjectWriter(resultModel);
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
                file.setError(e.getMessage());
                file.setState(State.ERROR);
                e.printStackTrace();
            }
            finally
            {
                if (file.getState() == State.EXECUTING)
                {
                    file.setState(State.COMPLETED);
                }
                queryFinnishedCallback.run();
            }
        });
    }

    private static void setupColumns(ResultModel model, QueryResult.QueryResultMetaData metaData)
    {
        List<String> columns = new ArrayList<>();
        columns.add("");
        for (String column : metaData.getColumns())
        {
            columns.add(column);
        }
        model.setColumns(columns.toArray(ArrayUtils.EMPTY_STRING_ARRAY));
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
        public void endRow()
        {
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

        private static class PairList extends ArrayList<Pair<String, Object>>
        {
        }
    }
}
