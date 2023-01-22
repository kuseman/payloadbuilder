package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.stream.Collectors.toList;

import java.util.ArrayList;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
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
        IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                .getObjectVectorBuilder(getType(arguments), rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            Object resultValue = result.valueAsObject(i);
            if (resultValue == null)
            {
                builder.put(null);
                continue;
            }
            Object value = VectorUtils.convert(resultValue);
            // Object vector can be transformed into a single row table
            if (value instanceof ObjectVector)
            {
                builder.put(convert((ObjectVector) value));
            }
            else if (value instanceof ValueVector)
            {
                builder.put(convert((ValueVector) value, resultValue));
            }
            else if (value instanceof TupleVector)
            {
                builder.put(value);
            }
            else
            {
                throw new IllegalArgumentException("Cannot cast " + resultValue + " to " + Column.Type.Table);
            }
        }

        return builder.build();
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

    private TupleVector convert(ValueVector array, Object resultValue)
    {
        // See if the vector consist of Map's or null then we can transform those into a Table
        int size = array.size();
        List<Map<String, Object>> maps = new ArrayList<>(size);
        for (int j = 0; j < size; j++)
        {
            Object el = array.valueAsObject(j);
            if (el == null)
            {
                continue;
            }
            else if (el instanceof Map)
            {
                @SuppressWarnings("unchecked")
                Map<String, Object> map = (Map<String, Object>) el;
                maps.add(map);
            }
            else
            {
                throw new IllegalArgumentException("Cannot cast " + resultValue + " to " + Column.Type.Table);
            }
        }

        if (maps.isEmpty())
        {
            return TupleVector.EMPTY;
        }

        Set<String> columns = new LinkedHashSet<>();

        for (Map<String, Object> map : maps)
        {
            columns.addAll(map.keySet());
        }

        final Schema schema = new Schema(columns.stream()
                .map(c -> Column.of(c, ResolvedType.of(Type.Any)))
                .collect(toList()));
        final int rowCount = maps.size();

        return new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public int getRowCount()
            {
                return rowCount;
            }

            @Override
            public ValueVector getColumn(int column)
            {
                final String name = schema.getColumns()
                        .get(column)
                        .getName();

                return new ValueVector()
                {
                    @Override
                    public ResolvedType type()
                    {
                        return ResolvedType.of(Type.Any);
                    }

                    @Override
                    public int size()
                    {
                        return rowCount;
                    }

                    @Override
                    public boolean isNull(int row)
                    {
                        return getAny(row) == null;
                    }

                    @Override
                    public Object getAny(int row)
                    {
                        return maps.get(row)
                                .get(name);
                    }
                };
            }
        };
    }
}
