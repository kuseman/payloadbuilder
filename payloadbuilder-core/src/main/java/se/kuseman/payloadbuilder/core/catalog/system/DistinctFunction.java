package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.utils.VectorUtils;

import it.unimi.dsi.fastutil.ints.Int2ObjectMap;
import it.unimi.dsi.fastutil.ints.Int2ObjectOpenHashMap;
import it.unimi.dsi.fastutil.ints.IntHash.Strategy;
import it.unimi.dsi.fastutil.ints.IntOpenCustomHashSet;

/** Distinct input */
class DistinctFunction extends ScalarFunctionInfo
{
    DistinctFunction(Catalog catalog)
    {
        super(catalog, "distinct", FunctionType.SCALAR);
    }

    @Override
    public int arity()
    {
        return 1;
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);

        // Non array types are already distinct by definition
        if (value.type()
                .getType() != Type.ValueVector)
        {
            return value;
        }

        final Int2ObjectMap<ValueVector> distinctIdsByRow = new Int2ObjectOpenHashMap<>(value.size());

        return new ValueVector()
        {

            @Override
            public ResolvedType type()
            {
                return value.type();
            }

            @Override
            public int size()
            {
                return value.size();
            }

            @Override
            public boolean isNullable()
            {
                return value.isNullable();
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row);
            }

            @Override
            public Object getValue(int row)
            {
                // Cache result to avoid calculating distinct multilpe times
                return distinctIdsByRow.computeIfAbsent(row, r ->
                {
                    ValueVector vv = (ValueVector) value.getValue(r);
                    int size = vv.size();

                    final ValueVector[] vectors = new ValueVector[] { vv };

                    IntOpenCustomHashSet set = new IntOpenCustomHashSet(size, new Strategy()
                    {
                        @Override
                        public int hashCode(int e)
                        {
                            return VectorUtils.hash(vectors, e);
                        }

                        @Override
                        public boolean equals(int a, int b)
                        {
                            return VectorUtils.equals(vectors, a, b);
                        }
                    });

                    // Populate set to get unique ordinals
                    for (int i = 0; i < size; i++)
                    {
                        set.add(i);
                    }

                    final int[] ordinals = set.toIntArray();

                    return new ValueVectorAdapter(vv)
                    {
                        @Override
                        public int size()
                        {
                            return ordinals.length;
                        }

                        @Override
                        public int getRow(int row)
                        {
                            return ordinals[row];
                        }
                    };
                });
            }
        };
    }
}
