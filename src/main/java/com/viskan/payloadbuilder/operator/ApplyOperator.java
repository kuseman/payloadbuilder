package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

/** Operator that applies the apply operator */
class ApplyOperator implements Operator
{
    private final Operator left;
    private final TableFunctionInfo functionInfo;
    private final Type type;
    private final List<Expression> arguments;
    private final RowMerger rowMerger;

    ApplyOperator(
            Operator left,
            Type type,
            TableFunctionInfo functionInfo,
            List<Expression> arguments,
            RowMerger rowMerger)
    {
        this.rowMerger = rowMerger;
        this.left = requireNonNull(left, "left");
        this.type = requireNonNull(type, "type");
        this.functionInfo = requireNonNull(functionInfo, "functionInfo");
        this.arguments = requireNonNull(arguments, "arguments");
    }

    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        final Iterator<Row> li = left.open(context);
        return new Iterator<Row>()
        {
            TableAlias tableAlias;
            int size = arguments.size();
            List<Object> args = new ArrayList<>(size);
            Iterator<Row> tableIt;
            Row currentOuter;
            Row next;

            @Override
            public Row next()
            {
                Row result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                return next != null || setNext();
            }

            boolean setNext()
            {
                while (next == null)
                {
                    if (tableIt == null)
                    {
                        if (!li.hasNext())
                        {
                            return false;
                        }

                        currentOuter = li.next();

                        // TODO: fix table alias, provide in context or in constructor
                        if (tableAlias == null)
                        {
                            tableAlias = TableAlias.of(currentOuter.getTableAlias(), functionInfo.getName(), "_" + functionInfo.getName());
                        }

                        for (int i = 0; i < size; i++)
                        {
                            args.set(i, arguments.get(i).eval(null, currentOuter));
                        }

                        tableIt = functionInfo.open(context, tableAlias, args);

                        // No hits from function and outer => just return row
                        if (type == Type.OUTER && !tableIt.hasNext())
                        {
                            next = currentOuter;
                        }

                        continue;
                    }
                    else if (!tableIt.hasNext())
                    {
                        tableIt = null;
                        continue;
                    }

                    next = rowMerger.apply(currentOuter, tableIt.next());
                }

                return next != null;
            }
        };
    }

    enum Type
    {
        CROSS, OUTER;
    }
}
