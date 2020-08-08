package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static org.kuse.payloadbuilder.core.DescribeUtils.CATALOG;
import static org.kuse.payloadbuilder.core.utils.MapUtils.entry;
import static org.kuse.payloadbuilder.core.utils.MapUtils.ofEntries;

import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.kuse.payloadbuilder.core.catalog.TableAlias;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Operator handling TVF's */
class TableFunctionOperator extends AOperator
{
    private final TableAlias tableAlias;
    private final TableFunctionInfo functionInfo;
    private final List<Expression> arguments;

    TableFunctionOperator(int nodeId, TableAlias tableAlias, TableFunctionInfo functionInfo, List<Expression> arguments)
    {
        super(nodeId);
        this.tableAlias = requireNonNull(tableAlias, "tableAlias");
        this.functionInfo = requireNonNull(functionInfo, "tableFunction");
        this.arguments = requireNonNull(arguments, "arguments");
    }

    @Override
    public String getName()
    {
        return "Function: " + functionInfo.getName();
    }

    @Override
    public Map<String, Object> getDescribeProperties()
    {
        return ofEntries(true,
                entry(CATALOG, functionInfo.getCatalog().getName()),
                entry("Arguments", arguments.stream().map(Object::toString).collect(toList())));
    }

    @Override
    public Iterator<Row> open(ExecutionContext context)
    {
        return functionInfo.open(context, tableAlias, new Arguments(arguments, context));
    }

    /** Lazy eval. of arguments provided to function implementation */
    private static class Arguments extends AbstractList<Object>
    {
        private final Object EMPTY = new Object();
        private final List<Expression> arguments;
        private final ExecutionContext context;
        private List<Object> evalArgs;

        private Arguments(List<Expression> arguments, ExecutionContext context)
        {
            this.arguments = arguments;
            this.context = context;
        }

        @Override
        public Object get(int index)
        {
            if (evalArgs == null)
            {
                evalArgs = new ArrayList<>();
            }
            if (evalArgs.size() <= index)
            {
                evalArgs.addAll(Collections.nCopies(index - evalArgs.size() + 1, EMPTY));
            }

            Object result = evalArgs.get(index);
            if (result == EMPTY)
            {
                result = arguments.get(index).eval(context);
                evalArgs.set(index, result);
            }

            return result;
        }

        @Override
        public int size()
        {
            return arguments.size();
        }

    }

    @Override
    public int hashCode()
    {
        return 17 +
            (37 * functionInfo.hashCode()) +
            (37 * arguments.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TableFunctionOperator)
        {
            TableFunctionOperator that = (TableFunctionOperator) obj;
            return nodeId == that.nodeId
                && tableAlias.isEqual(that.tableAlias)
                && functionInfo.equals(that.functionInfo)
                && arguments.equals(that.arguments);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return String.format("%s (ID: %d, %s)", functionInfo.getName(), nodeId, arguments.stream().map(Object::toString).collect(joining(", ")));
    }
}