package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Operator handling TVF's */
public class TableFunctionOperator implements Operator
{
    private final TableAlias tableAlias;
    private final TableFunctionInfo functionInfo;
    private final List<Expression> arguments;

    public TableFunctionOperator(TableAlias tableAlias, TableFunctionInfo functionInfo, List<Expression> arguments)
    {
        this.tableAlias = requireNonNull(tableAlias, "tableAlias");
        this.functionInfo = requireNonNull(functionInfo, "tableFunction");
        this.arguments = requireNonNull(arguments, "arguments");
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        List<Object> args = emptyList();

        if (arguments.size() > 0)
        {
            args = new ArrayList<>(arguments.size());
            for (Expression arg : arguments)
            {
                args.add(arg.eval(context.getEvaluationContext(), context.getParentRow()));
            }
        }
        return functionInfo.open(context, tableAlias, args);
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
            TableFunctionOperator tfo = (TableFunctionOperator) obj;
            return tableAlias.isEqual(tfo.tableAlias)
                &&
                functionInfo.equals(tfo.functionInfo)
                &&
                arguments.equals(tfo.arguments);
        }
        return super.equals(obj);
    }

    @Override
    public String toString()
    {
        return String.format("%s (%s)", functionInfo.getName(), arguments.stream().map(Object::toString).collect(joining(", ")));
    }
}
