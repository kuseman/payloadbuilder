package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.operator.ComplexValue;
import org.kuse.payloadbuilder.core.operator.DescribableNode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Operator.RowIterator;
import org.kuse.payloadbuilder.core.operator.Projection;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.Select.For;

/**
 * A resolved {@link UnresolvedSubQueryExpression} with a built operator tree and projection
 **/
public class SubQueryExpression extends Expression implements DescribableNode
{
    private final Operator operator;
    private final String[] columns;
    private final Projection[] projections;
    private final For forOutput;

    public SubQueryExpression(Operator operator, String[] columns, Projection[] projections, Select.For forOutput)
    {
        this.operator = requireNonNull(operator, "operator");
        this.columns = requireNonNull(columns, "columns");
        this.projections = requireNonNull(projections, "projection");
        this.forOutput = forOutput;
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        throw new RuntimeException("A SubQueryExpression should not be visited");
    }

    @Override
    public String getName()
    {
        return "Subquery";
    }

    @Override
    public List<DescribableNode> getChildNodes()
    {
        List<DescribableNode> children = new ArrayList<>(projections.length + 1);
        children.add(operator);
        children.addAll(asList(projections));
        return children;
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        //CSOFF
        return new ComplexValue()
        //CSON
        {
            @Override
            public void write(OutputWriter outputWriter, ExecutionContext context)
            {
                RowIterator it = operator.open(context);

                begin(outputWriter);

                HierarchyTuple tuple = new HierarchyTuple(context.getTuple());

                boolean writeFieldName = forOutput == For.OBJECT || forOutput == For.OBJECT_ARRAY;
                int size = columns.length;
                while (it.hasNext())
                {
                    tuple.setCurrent(it.next());
                    // Re-set the context tuple in case we have nested complex values
                    context.setTuple(tuple);

                    if (forOutput == For.OBJECT_ARRAY)
                    {
                        outputWriter.startObject();
                    }

                    // Write the select items
                    for (int i = 0; i < size; i++)
                    {
                        Projection projection = projections[i];
                        if (writeFieldName)
                        {
                            outputWriter.writeFieldName(columns[i]);
                        }

                        projection.writeValue(outputWriter, context);
                    }

                    if (forOutput == For.OBJECT_ARRAY)
                    {
                        outputWriter.endObject();
                    }

                    // Objects can only have one "row"
                    if (forOutput == For.OBJECT)
                    {
                        break;
                    }
                }

                end(outputWriter);
            }

            private void begin(OutputWriter writer)
            {
                if (forOutput == For.ARRAY || forOutput == For.OBJECT_ARRAY)
                {
                    writer.startArray();
                }
                else
                {
                    writer.startObject();
                }
            }

            private void end(OutputWriter writer)
            {
                if (forOutput == For.ARRAY || forOutput == For.OBJECT_ARRAY)
                {
                    writer.endArray();
                }
                else
                {
                    writer.endObject();
                }
            }
        };
    }

    @Override
    public int hashCode()
    {
        return operator.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof SubQueryExpression)
        {
            SubQueryExpression that = (SubQueryExpression) obj;
            return operator.equals(that.operator)
                && Arrays.equals(columns, that.columns)
                && Arrays.equals(projections, that.projections)
                && forOutput == that.forOutput;
        }
        return false;
    }

    /**
     * Tuple that is used to temporary connect 2 tuples during a complex value iteration
     *
     * <pre>
     *  select a.col,
     *         (select a.col2, b.col3 from tableB for array) arr
     *  from tableA a
     *
     *  To be able to "reach" alias b from within the complex value arr
     *  we need to temporary connect the outer tuple (tableA) before evaluating
     * </pre>
     **/
    private static class HierarchyTuple implements Tuple
    {
        private final Tuple parent;
        private Tuple current;

        private HierarchyTuple(Tuple parent)
        {
            this.parent = parent;
        }

        private void setCurrent(Tuple current)
        {
            this.current = current;
        }

        @Override
        public int getTupleOrdinal()
        {
            return current.getTupleOrdinal();
        }

        @Override
        public Tuple getTuple(int tupleOrdinal)
        {
            /*
             * select col
             * ,      (
             *              select a.col1, b.col2, c.col
             *              from tableB b                       ordinal = 1 (current)
             *              inner join tableC c                 ordinal = 2
             *              for object
             *        ) obj
             * from tableA a                                    ordinal = 0 (parent)
             */

            int currentTupleOrdinal = current.getTupleOrdinal();
            if (currentTupleOrdinal >= 0 && tupleOrdinal >= current.getTupleOrdinal())
            {
                return current.getTuple(tupleOrdinal);
            }

            return parent.getTuple(tupleOrdinal);
        }

        @Override
        public int getColumnCount()
        {
            // The parent is only for a hierarchy navigation and should
            // not be involved when fetching columns
            return current.getColumnCount();
        }

        @Override
        public int getColumnOrdinal(String column)
        {
            return current.getColumnOrdinal(column);
        }

        @Override
        public String getColumn(int ordinal)
        {
            return current.getColumn(ordinal);
        }

        @Override
        public Object getValue(int ordinal)
        {
            return current.getValue(ordinal);
        }
    }
}
