package se.kuseman.payloadbuilder.core.parser;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.codegen.CodeGeneratorContext;
import se.kuseman.payloadbuilder.api.codegen.ProjectionCode;
import se.kuseman.payloadbuilder.api.operator.DescribableNode;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator;
import se.kuseman.payloadbuilder.api.operator.Tuple;
import se.kuseman.payloadbuilder.core.operator.ATupleIterator;
import se.kuseman.payloadbuilder.core.operator.ComplexValue;
import se.kuseman.payloadbuilder.core.operator.ExecutionContext;
import se.kuseman.payloadbuilder.core.operator.Projection;
import se.kuseman.payloadbuilder.core.operator.StatementContext;
import se.kuseman.payloadbuilder.core.parser.Select.For;

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
    public Object eval(IExecutionContext context)
    {
        return complexValue;
    }

    @Override
    public ProjectionCode generateProjectionCode(CodeGeneratorContext context)
    {
        context.addImport("se.kuseman.payloadbuilder.core.parser.SubQueryExpression");
        context.addImport("java.lang.Runnable");
        int index = context.addReference(this);

        boolean writeFieldName = forOutput == For.OBJECT
                || forOutput == For.OBJECT_ARRAY;

        StringBuilder sb = new StringBuilder();

        sb.append("((SubQueryExpression) references[")
                .append(index)
                .append("]).write(writer, context, new Runnable() {\n");
        sb.append("  public void run() {\n");

        sb.append("    Tuple tuple = context.getStatementContext().getTuple();\n");
        int size = columns.length;
        for (int i = 0; i < size; i++)
        {
            if (writeFieldName)
            {
                sb.append("    writer.writeFieldName(\"")
                        .append(columns[i])
                        .append("\");\n");
            }

            if (i > 0)
            {
                // Re-set context tuple on each iterator since it can change with nested projections
                sb.append("context.getStatementContext().setTuple(tuple);\n");
            }
            context.setTupleFieldName("tuple");
            sb.append(projections[i].generateCode(context)
                    .getCode())
                    .append("\n");
        }

        sb.append("  }\n");
        sb.append("});\n");
        ProjectionCode code = context.getProjectionCode();
        code.setCode(sb.toString());
        return code;
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
    // CSOFF
    public void write(OutputWriter outputWriter, IExecutionContext context, Runnable projectionAction)
    // CSON
    {
        begin(outputWriter);

        StatementContext sctx = (StatementContext) context.getStatementContext();
        Tuple contextTuple = sctx.getTuple();
        // Use a tuple iterator to easier handle TupleList etc.
        ATupleIterator it = new ATupleIterator(operator.open(context))
        {
            @Override
            public Tuple process(Tuple tuple)
            {
                if (forOutput == For.OBJECT_ARRAY)
                {
                    outputWriter.startObject();
                }

                sctx.setTuple(tuple);
                projectionAction.run();

                if (forOutput == For.OBJECT_ARRAY)
                {
                    outputWriter.endObject();
                }

                return tuple;
            }
        };

        try
        {
            while (it.hasNext())
            {
                it.next();
                // Objects can only have one "row"
                if (forOutput == For.OBJECT)
                {
                    break;
                }
            }
        }
        finally
        {
            it.close();
        }
        end(outputWriter);
        sctx.setTuple(contextTuple);
    }

    /** Complex value implementation for this sub query expression */
    private class SubQueryComplexValue implements ComplexValue
    {
        @Override
        public void write(OutputWriter outputWriter, ExecutionContext context)
        {
            StatementContext sctx = context.getStatementContext();
            boolean writeFieldName = forOutput == For.OBJECT
                    || forOutput == For.OBJECT_ARRAY;
            int size = columns.length;
            SubQueryExpression.this.write(outputWriter, context, () ->
            {
                Tuple tuple = sctx.getTuple();

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
                        sctx.setTuple(tuple);
                    }
                    Projection projection = projections[i];
                    projection.writeValue(outputWriter, context);
                }
            });
        }
    }

    private void begin(OutputWriter writer)
    {
        if (forOutput == For.ARRAY
                || forOutput == For.OBJECT_ARRAY)
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
        if (forOutput == For.ARRAY
                || forOutput == For.OBJECT_ARRAY)
        {
            writer.endArray();
        }
        else
        {
            writer.endObject();
        }
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
}