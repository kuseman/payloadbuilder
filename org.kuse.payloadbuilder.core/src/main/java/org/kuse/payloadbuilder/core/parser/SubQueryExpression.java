package org.kuse.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ProjectionCode;
import org.kuse.payloadbuilder.core.operator.ComplexValue;
import org.kuse.payloadbuilder.core.operator.DescribableNode;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.NoOpTuple;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.operator.Operator.RowList;
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
    private final ComplexValue complexValue;

    public SubQueryExpression(Operator operator, String[] columns, Projection[] projections, Select.For forOutput)
    {
        this.operator = requireNonNull(operator, "operator");
        this.columns = requireNonNull(columns, "columns");
        this.projections = requireNonNull(projections, "projection");
        this.forOutput = forOutput;
        this.complexValue = new SubQueryComplexValue();
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
        return complexValue;
    }

    @Override
    public ProjectionCode generateProjectionCode(CodeGeneratorContext context)
    {
        context.addImport("org.kuse.payloadbuilder.core.parser.SubQueryExpression");
        context.addImport("java.lang.Runnable");
        int index = context.addReference(this);
        ProjectionCode code = context.getProjectionCode();

        boolean writeFieldName = forOutput == For.OBJECT || forOutput == For.OBJECT_ARRAY;
        int size = columns.length;

        StringBuilder sb = new StringBuilder();

        sb.append("((SubQueryExpression) references[").append(index).append("]).write(writer, context, new Runnable() {\n");
        sb.append("  public void run() {\n");

        sb.append("    Tuple tuple = context.getStatementContext().getTuple();\n");
        for (int i = 0; i < size; i++)
        {
            if (writeFieldName)
            {
                sb.append("    writer.writeFieldName(\"").append(columns[i]).append("\");\n");
            }

            if (i > 0)
            {
                // Re-set context tuple on each iterator since it can change with nested projections
                sb.append("context.getStatementContext().setTuple(tuple);\n");
            }
            context.setTupleFieldName("tuple");
            sb.append(projections[i].generateCode(context).getCode()).append("\n");
        }

        sb.append("  }\n");
        sb.append("});\n");

        code.setCode(sb.toString());
        return code;
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
     * <pre>
     * Method used when having a code generated projection. To keep the operator logic
     * separate from the projection logic
     * </pre>
     *
     * @param outputWriter Writer
     * @param context Execution context
     * @param projectionAction Action that is called for each tuple in the sub query.
     */
    //CSOFF
    public void write(OutputWriter outputWriter, ExecutionContext context, Runnable projectionAction)
    //CSON
    {
        begin(outputWriter);

        Tuple contextTuple = context.getStatementContext().getTuple();
        HierarchyTuple hierarchyTuple = null;

        TupleIterator it = operator.open(context);
        RowList list = it instanceof RowList ? (RowList) it : null;
        int index = 0;
        boolean complete = list == null
            ? !it.hasNext()
            : index >= list.size();

        while (!complete)
        {
            Tuple currentTuple = list == null ? it.next() : list.get(index++);

            // No op tuple, no need to create a hierarchy tuple
            // just set the context tuple
            if (currentTuple == NoOpTuple.NO_OP)
            {
                context.getStatementContext().setTuple(contextTuple);
            }
            else
            {
                if (hierarchyTuple == null)
                {
                    hierarchyTuple = new HierarchyTuple(contextTuple);
                }
                hierarchyTuple.setCurrent(currentTuple);
                // Re-set the context tuple in case we have nested complex values
                context.getStatementContext().setTuple(hierarchyTuple);
            }

            if (forOutput == For.OBJECT_ARRAY)
            {
                outputWriter.startObject();
            }

            projectionAction.run();

            if (forOutput == For.OBJECT_ARRAY)
            {
                outputWriter.endObject();
            }

            // Objects can only have one "row"
            if (forOutput == For.OBJECT)
            {
                break;
            }

            complete = list == null
                ? !it.hasNext()
                : index >= list.size();
        }

        end(outputWriter);
        context.getStatementContext().setTuple(contextTuple);
    }

    /** Complext value implementation for this sub query expression */
    private class SubQueryComplexValue implements ComplexValue
    {
        @Override
        public void write(OutputWriter outputWriter, ExecutionContext context)
        {
            boolean writeFieldName = forOutput == For.OBJECT || forOutput == For.OBJECT_ARRAY;
            int size = columns.length;
            SubQueryExpression.this.write(outputWriter, context, () ->
            {
                Tuple tuple = context.getStatementContext().getTuple();

                // Write the select items
                for (int i = 0; i < size; i++)
                {
                    if (writeFieldName)
                    {
                        outputWriter.writeFieldName(columns[i]);
                    }

                    if (i > 0)
                    {
                        // Re-set tuple after each iterator if it's been altered
                        context.getStatementContext().setTuple(tuple);
                    }
                    Projection projection = projections[i];
                    projection.writeValue(outputWriter, context);
                }
            });
        }
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
    static class HierarchyTuple implements Tuple
    {
        private final Tuple parent;
        private Tuple current;

        HierarchyTuple(Tuple parent)
        {
            this.parent = parent;
        }

        void setCurrent(Tuple current)
        {
            this.current = current;
        }

        @Override
        public int getColumnOrdinal(String column)
        {
            if (current == null)
            {
                return -1;
            }
            return current.getColumnOrdinal(column);
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
             *
             * select (
             *       id                     <------ 1
             *     , c.id                   <------ 2
             *     from open_rows(b)        <------ 1 (current)
             *   ) obj
             * from tableA a                0         (composite parent)
             * inner join tableB b          1
             *  on ...
             * inner join tableC c          2
             *  on ...
             *
             */
            if (tupleOrdinal == -1
                || current == null
                || tupleOrdinal == current.getTupleOrdinal())
            {
                return current;
            }

            // Target might be a descendant of current
            if (tupleOrdinal > current.getTupleOrdinal())
            {
                Tuple result = current.getTuple(tupleOrdinal);
                if (result != null)
                {
                    return result;
                }
            }
            // if not found delegate to parent
            return parent.getTuple(tupleOrdinal);
        }

        @Override
        public Object getValue(int columnOrdinal)
        {
            if (current == null)
            {
                return null;
            }
            return current.getValue(columnOrdinal);
        }

        @Override
        public int getTupleOrdinal()
        {
            throw new IllegalArgumentException("Not implemented");
        }

        @Override
        public int getColumnCount()
        {
            throw new IllegalArgumentException("Not implemented");
        }

        @Override
        public String getColumn(int columnOrdinal)
        {
            throw new IllegalArgumentException("Not implemented");
        }
    }
}
