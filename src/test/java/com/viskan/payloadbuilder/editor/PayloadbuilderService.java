package com.viskan.payloadbuilder.editor;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.analyze.OperatorBuilder;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.editor.QueryFileModel.State;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.OutputWriter;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.parser.ParseException;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;
import com.viskan.payloadbuilder.parser.tree.Query;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;
import java.util.concurrent.Executor;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.apache.commons.lang3.tuple.Pair;

/** Class the executes queries etc. */
class PayloadbuilderService
{
    private static final Executor EXECUTOR = Executors.newFixedThreadPool(Runtime.getRuntime().availableProcessors() * 2);

    /** Execute query for provider query file */
    static void executeQuery(
            QueryFileModel file,
            ResultModel resultModel,
            String queryString,
            CatalogRegistry registry)
    {

        EXECUTOR.execute(() ->
        {
            file.setState(State.EXECUTING);
            StopWatch sw = new StopWatch();
            sw.start();
            try
            {
                QueryParser parser = new QueryParser();
                Query query = parser.parseQuery(registry, queryString);

                Pair<Operator, Projection> pair = OperatorBuilder.create(registry, query);
                Operator op = pair.getKey();
                Projection pr = pair.getValue();

                setupColumns(resultModel, pr);

                ObjectWriter writer = new ObjectWriter();
                OperatorContext context = new OperatorContext();
                if (op != null)
                {
                    Iterator<Row> it = op.open(context);
                    while (it.hasNext())
                    {
                        // Check if query should be aborted
                        if (file.getState() == State.ABORTED)
                        {
                            break;
                        }

                        Row row = it.next();
                        pr.writeValue(writer, context, row);
                        resultModel.addRow(writer.getValue(resultModel.getRowCount() + 1));
                        file.setExecutionTime(sw.getTime(TimeUnit.MILLISECONDS));
                    }
                }
                else
                {
                    TableAlias alias = TableAlias.of(null, QualifiedName.of("table"), "t");
                    Row row = Row.of(alias, 0, ArrayUtils.EMPTY_OBJECT_ARRAY);
                    pr.writeValue(writer, context, row);
                    resultModel.addRow(writer.getValue(1));
                }

                resultModel.done();

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
            }
        });
    }

    private static void setupColumns(ResultModel model, Projection pr)
    {
        List<String> columns = new ArrayList<>();
        columns.add("");
        for (String column : pr.getColumns())
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
        
        /** Returns written value and clears state */
        public Object[] getValue(int rowNumber)
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
        {}
    }
}
