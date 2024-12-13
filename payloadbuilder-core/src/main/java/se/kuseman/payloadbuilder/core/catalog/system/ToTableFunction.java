package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.MutableValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.execution.ValueVectorAdapter;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;

/** Function toTable. Tries to convert input into a {@link TupleVector}. */
class ToTableFunction extends ScalarFunctionInfo
{
    ToTableFunction()
    {
        super("totable", FunctionType.SCALAR);
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        Schema schema = Schema.EMPTY;
        return ResolvedType.table(schema);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        final ValueVector result = arguments.get(0)
                .eval(input, context);

        int rowCount = input.getRowCount();

        MutableValueVector resultVector = context.getVectorFactory()
                .getMutableVector(getType(arguments), rowCount);
        for (int i = 0; i < rowCount; i++)
        {
            Object resultValue = result.valueAsObject(i);
            if (resultValue == null)
            {
                resultVector.setNull(i);
                continue;
            }
            Object value = VectorUtils.convert(resultValue);
            // Object vector can be transformed into a single row table
            if (value instanceof ObjectVector object)
            {
                resultVector.setTable(i, convert(object));
            }
            else if (value instanceof ValueVector vector)
            {
                resultVector.setTable(i, convert(vector, resultValue));
            }
            else if (value instanceof TupleVector vector)
            {
                resultVector.setTable(i, vector);
            }
            else
            {
                throw new IllegalArgumentException("Cannot cast " + resultValue + "(" + resultValue.getClass() + ") to " + Column.Type.Table);
            }
        }

        return resultVector;
    }

    private TupleVector convert(final ObjectVector object)
    {
        return new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return object.getSchema();
            }

            @Override
            public int getRowCount()
            {
                return 1;
            }

            @Override
            public ValueVector getColumn(int column)
            {
                return new ValueVectorAdapter(object.getValue(column))
                {
                    @Override
                    public int size()
                    {
                        return 1;
                    }

                    @Override
                    protected int getRow(int row)
                    {
                        return object.getRow();
                    }
                };
            }
        };
    }

    @SuppressWarnings("unchecked")
    private TupleVector convert(ValueVector array, Object resultValue)
    {
        // If all items are maps we create a table with the union of all columns
        // else we add all values to a single Value column
        int size = array.size();
        List<Object> values = new ArrayList<>(size);
        boolean allMaps = true;
        for (int j = 0; j < size; j++)
        {
            Object el = array.valueAsObject(j);
            values.add(el);
            allMaps = allMaps
                    && el instanceof Map;
        }

        if (values.isEmpty())
        {
            return TupleVector.EMPTY;
        }

        final boolean maps;
        final List<String> columns;
        if (allMaps)
        {
            columns = values.stream()
                    .map(m -> (Map<String, Object>) m)
                    .flatMap(m -> m.keySet()
                            .stream())
                    .distinct()
                    .collect(toList());
            maps = true;
        }
        else
        {
            columns = List.of("Value");
            maps = false;
        }

        final List<Object> theBatch = values;
        final Schema schema = new Schema(columns.stream()
                .map(c -> new Column(c, ResolvedType.of(Type.Any)))
                .toList());

        return new ObjectTupleVector(schema, values.size(), (row, col) ->
        {
            Object rowValue = theBatch.get(row);

            if (maps)
            {
                String columnName = columns.get(col);
                return ((Map<String, Object>) rowValue).get(columnName);
            }

            return rowValue;
        });
    }
}
