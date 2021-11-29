package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.collections4.IteratorUtils.toList;

import java.io.IOException;
import java.io.Reader;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Stack;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.mutable.MutableDouble;
import org.apache.commons.lang3.mutable.MutableFloat;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.commons.lang3.mutable.MutableLong;
import org.kuse.payloadbuilder.core.OutputWriter;

/** Utils for evaluation of expressions */
public final class EvalUtils
{
    private EvalUtils()
    {
    }

    /**
     * Unwraps provided objects.
     *
     * <pre>
     * There are special types provided by functions in the pipe when evaluating expression
     * and these objects need to be processed to make something useful of them.
     * These are:
     *   - Iterator (make a list of it)
     *   - Reader (Read to string)
     *   - Mutable numbers
     * </pre>
     */
    @SuppressWarnings("unchecked")
    public static Object unwrap(ExecutionContext context, Object object)
    {
        if (object instanceof Iterator)
        {
            return toList((Iterator<Object>) object);
        }
        else if (object instanceof Reader)
        {
            try
            {
                return IOUtils.toString((Reader) object);
            }
            catch (IOException e)
            {
                throw new RuntimeException("Error trying to read Reader", e);
            }
        }
        else if (object instanceof ComplexValue)
        {
            // Build the complex value
            // This is a sub query expression inside a sub query table source
            /*
             * select *
             * from
             * (
             *      select col,
             *             (                <--- This value is lazy and needs to be materialized
             *               select
             *                  col2,
             *                  col3
             *               for object
             *             ) obj
             *      from table
             * ) x
             * where x.obj.col3 = 'hello'
             *
             *
             */
            ComplexValue value = (ComplexValue) object;
            ComplexValueWriter writer = new ComplexValueWriter(context);
            value.write(writer, context);
            return writer.getValue();
        }
        // Unwrap mutables
        else if (object instanceof MutableDouble)
        {
            return ((MutableDouble) object).doubleValue();
        }
        else if (object instanceof MutableFloat)
        {
            return ((MutableFloat) object).floatValue();
        }
        else if (object instanceof MutableLong)
        {
            return ((MutableLong) object).longValue();
        }
        else if (object instanceof MutableInt)
        {
            return ((MutableInt) object).intValue();
        }

        return object;
    }

    /** Writer that builds a complex value. */
    public static class ComplexValueWriter implements OutputWriter
    {
        private final ExecutionContext context;
        private final Stack<Object> current;
        private String fieldName;

        ComplexValueWriter(ExecutionContext context)
        {
            this.context = context;
            this.current = new Stack<>();
        }

        Object getValue()
        {
            return current.peek();
        }

        @Override
        public void writeFieldName(String name)
        {
            fieldName = name;
        }

        @Override
        public void writeValue(Object value)
        {
            requireNonNull(current);
            Object obj = current.peek();
            if (obj instanceof Map)
            {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) obj;
                map.put(fieldName, unwrap(context, value));
            }
            else
            {
                @SuppressWarnings("unchecked")
                List<Object> list = (List<Object>) obj;
                list.add(unwrap(context, value));
            }
        }

        @Override
        public void startObject()
        {
            current.push(new LinkedHashMap<>());
        }

        @Override
        public void endObject()
        {
            // Keep the first object on the stack
            if (current.size() > 1)
            {
                current.pop();
            }
        }

        @Override
        public void startArray()
        {
            current.push(new ArrayList<>());
        }

        @Override
        public void endArray()
        {
            // Keep the first object on the stack
            if (current.size() > 1)
            {
                current.pop();
            }
        }
    }
}
