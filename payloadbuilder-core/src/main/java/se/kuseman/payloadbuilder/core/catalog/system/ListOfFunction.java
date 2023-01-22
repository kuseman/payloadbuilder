package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Function listOf. Creates a list of provided arguments */
class ListOfFunction extends ScalarFunctionInfo
{
    ListOfFunction(Catalog catalog)
    {
        super(catalog, "listOf", FunctionType.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Creates a list of provided arguments." + System.lineSeparator() + "ie. listOf(1,2, true, 'string')";
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        // TODO: if all arguments are the same we can type the value vector
        return ResolvedType.valueVector(ResolvedType.of(Type.Any));
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        /*
         * @formatter:off
         * Input:
         * col1: [1,2,3]
         * col2: [4,5,6]
         * 
         * listof(col1, true)
         * 
         * arg0: [1,2,3]
         * arg1: [true, true, true]
         * 
         * 
         * Result
         * 0: [1, true]
         * 1: [2, true]
         * 2: [3, true]
         * @formatter:on
         */

        final int size = arguments.size();
        ValueVector[] vectors = new ValueVector[size];
        for (int i = 0; i < size; i++)
        {
            vectors[i] = arguments.get(i)
                    .eval(input, context);
        }

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return ResolvedType.valueVector(ResolvedType.of(Type.Any));
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNullable()
            {
                return false;
            }

            @Override
            public boolean isNull(int row)
            {
                return false;
            }

            @Override
            public Object getValue(int row)
            {
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
                        return size;
                    }

                    @Override
                    public boolean isNull(int argumentRow)
                    {
                        if (argumentRow >= vectors.length)
                        {
                            System.out.println();
                        }
                        return vectors[argumentRow].isNull(row);
                    }

                    @Override
                    public Object getValue(int argumentRow)
                    {
                        // TOOD: Boxing here
                        return vectors[argumentRow].valueAsObject(row);
                    }
                };
            }
        };
    }
    //
    // @Override
    // public Object eval(IExecutionContext ctx, String catalogAlias, List<? extends IExpression> arguments)
    // {
    // ExecutionContext context = (ExecutionContext) ctx;
    // int size = arguments.size();
    // if (size <= 0)
    // {
    // return emptyList();
    // }
    // List<Object> result = new ArrayList<>(size);
    // for (int i = 0; i < size; i++)
    // {
    // Object object = arguments.get(i)
    // .eval(context);
    // result.add(EvalUtils.unwrap(context, object));
    // }
    // return result;
    // }
}
