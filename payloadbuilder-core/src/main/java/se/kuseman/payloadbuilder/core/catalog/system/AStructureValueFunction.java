package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector.OutputWritable;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.StatementContext;
import se.kuseman.payloadbuilder.core.physicalplan.ProjectionUtils;

/**
 * Aggregation that transforms input into an object/array/object-array structure. Result is an {@link Type#OutputWritable} that can be lazily written with an {@link OutputWritable}
 */
class AStructureValueFunction
{
    static final AStructureValueFunction OBJECT = new AStructureValueFunction(OutputType.OBJECT);
    static final AStructureValueFunction OBJECT_ARRAY = new AStructureValueFunction(OutputType.OBJECT_ARRAY);
    static final AStructureValueFunction ARRAY = new AStructureValueFunction(OutputType.ARRAY);

    private final OutputType outputType;

    private AStructureValueFunction(OutputType outputType)
    {
        this.outputType = requireNonNull(outputType, "outputType");
    }

    public OutputType getOutputType()
    {
        return outputType;
    }

    enum OutputType
    {
        OBJECT,
        OBJECT_ARRAY,
        ARRAY
    }

    /** Operator eval */
    ValueVector eval(TupleVector input, IExecutionContext context)
    {
        int rowCount = input.getRowCount();
        if (rowCount == 0)
        {
            return ValueVector.literalNull(ResolvedType.of(Type.OutputWritable), 0);
        }

        Schema schema = input.getSchema();
        int size = schema.getSize();
        ValueVector[] valueVectors = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            valueVectors[i] = input.getColumn(i);
        }
        return ValueVector.literalObject(ResolvedType.of(Type.OutputWritable), createOutputWritable(schema, rowCount, valueVectors));
    }

    /** Aggregate eval */
    ValueVector eval(ValueVector groups, List<? extends IExpression> expressions, IExecutionContext context)
    {
        int size = groups.size();
        return new ValueVector()
        {
            @Override
            public boolean isNullable()
            {
                return true;
            }

            @Override
            public boolean isNull(int row)
            {
                // No rows => null since we don't want an empty array/object etc.
                // since it will be hard to create a default value from that. With null
                // a ISNULL can be used to provide a default
                TupleVector vector = (TupleVector) groups.getValue(row);
                return vector.getRowCount() == 0;
            }

            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.OutputWritable);
            }

            @Override
            public int size()
            {
                return size;
            }

            @Override
            public Object getValue(int row)
            {
                TupleVector vector = (TupleVector) groups.getValue(row);
                if (vector.getRowCount() == 0)
                {
                    return null;
                }
                return create(context, vector, expressions);
            }
        };
    }

    ValueVector evalScalar(TupleVector input, List<? extends IExpression> expressions, IExecutionContext context)
    {
        ValueVector groups = ValueVector.literalObject(ResolvedType.tupleVector(input.getSchema()), input, 1);
        return eval(groups, expressions, context);
    }

    private OutputWritable create(IExecutionContext context, TupleVector tupleVector, List<? extends IExpression> expressions)
    {
        boolean hasAsterisks = expressions.stream()
                .anyMatch(e -> e.getColumnReference() != null
                        && e.getColumnReference()
                                .getType() == ColumnReference.Type.ASTERISK);

        TupleVector outerTupleVector = ((StatementContext) context.getStatementContext()).getOuterTupleVector();
        Schema inputSchema = tupleVector.getSchema();
        Schema outerSchema = outerTupleVector != null ? outerTupleVector.getSchema()
                : null;

        final List<? extends IExpression> actualExpressions = hasAsterisks ? ProjectionUtils.expandExpressions(expressions, outerSchema, inputSchema)
                : expressions;
        final Schema schema = ProjectionUtils.createSchema(inputSchema, expressions, false);

        int size = actualExpressions.size();
        ValueVector[] valueVectors = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            valueVectors[i] = expressions.get(i)
                    .eval(tupleVector, context);
        }

        return createOutputWritable(schema, tupleVector.getRowCount(), valueVectors);
    }

    private OutputWritable createOutputWritable(Schema schema, int rowCount, ValueVector[] valueVectors)
    {
        final List<Column> columns = schema.getColumns();
        final int size = columns.size();

        return new OutputWritable()
        {
            @Override
            public void write(OutputWriter outputWriter, IExecutionContext context)
            {
                boolean isArray = outputType == OutputType.ARRAY
                        || outputType == OutputType.OBJECT_ARRAY;
                if (isArray)
                {
                    outputWriter.startArray();
                }
                // Create an object array for each row
                for (int row = 0; row < rowCount; row++)
                {
                    if (outputType != OutputType.ARRAY)
                    {
                        outputWriter.startObject();
                    }
                    for (int j = 0; j < size; j++)
                    {
                        Column column = columns.get(j);
                        ValueVector vv = valueVectors[j];

                        // Scalar array doesn't have field names
                        if (outputType != OutputType.ARRAY)
                        {
                            outputWriter.writeFieldName(column.getName());
                        }
                        vv.write(row, outputWriter, context);
                    }
                    if (outputType != OutputType.ARRAY)
                    {
                        outputWriter.endObject();
                    }
                    // Object only uses one row
                    if (outputType == OutputType.OBJECT)
                    {
                        break;
                    }
                }
                if (isArray)
                {
                    outputWriter.endArray();
                }
            }

            @Override
            public String toString()
            {
                return "OutputWritable: " + outputType;
            }
        };

    }
}