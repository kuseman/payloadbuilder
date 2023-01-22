package se.kuseman.payloadbuilder.core.catalog.system;

import java.util.List;
import java.util.NoSuchElementException;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleIterator;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Range table valued function that emits row in range */
class RangeFunction extends TableFunctionInfo
{
    private static final Schema SCHEMA = Schema.of(Column.of("Value", ResolvedType.of(Type.Int)));

    RangeFunction(Catalog catalog)
    {
        super(catalog, "range");
    }

    @Override
    public Schema getSchema(List<? extends IExpression> arguments)
    {
        return SCHEMA;
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments, IDatasourceOptions options)
    {
        ValueVector vv;

        int from = 0;
        int to = -1;
        // to
        if (arguments.size() <= 1)
        {
            vv = eval(context, arguments.get(0));
            if (vv.isNull(0))
            {
                throw new IllegalArgumentException("From argument to range cannot be null.");
            }
            to = vv.getInt(0);
        }
        // from, to
        else if (arguments.size() <= 2)
        {
            vv = eval(context, arguments.get(0));
            if (vv.isNull(0))
            {
                throw new IllegalArgumentException("From argument to range cannot be null.");
            }
            from = vv.getInt(0);
            vv = eval(context, arguments.get(1));
            if (vv.isNull(0))
            {
                throw new IllegalArgumentException("To argument to range cannot be null.");
            }
            to = vv.getInt(0);
        }

        final int start = from;
        final int stop = to;
        final int rowCount = Math.max(stop - start, 0);
        final int batchSize = options.getBatchSize(context);

        if (rowCount <= batchSize)
        {
            return TupleIterator.singleton(getVector(start, stop));
        }

        final int batchCount = rowCount / batchSize;
        return new TupleIterator()
        {
            int batchNumber = 0;
            TupleVector next;

            @Override
            public TupleVector next()
            {
                if (next == null)
                {
                    throw new NoSuchElementException();
                }
                TupleVector result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                return setNext();
            }

            private boolean setNext()
            {
                while (next == null)
                {
                    if (batchNumber > batchCount)
                    {
                        return false;
                    }

                    int batchStart = start + (batchSize * batchNumber);
                    int batchStop = Math.min(batchStart + batchSize, stop);
                    next = getVector(batchStart, batchStop);
                    batchNumber++;
                }
                return true;
            }
        };
    }

    private TupleVector getVector(int start, int stop)
    {
        int rowCount = Math.max(stop - start, 0);
        return new TupleVector()
        {
            @Override
            public Schema getSchema()
            {
                return SCHEMA;
            }

            @Override
            public int getRowCount()
            {
                return rowCount;
            }

            @Override
            public ValueVector getColumn(final int column)
            {
                return new ValueVector()
                {
                    @Override
                    public ResolvedType type()
                    {
                        return ResolvedType.of(Type.Int);
                    }

                    @Override
                    public int size()
                    {
                        return rowCount;
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
                    public int getInt(int row)
                    {
                        return start + row;
                    }

                    @Override
                    public Object getValue(int row)
                    {
                        throw new IllegalArgumentException("getValue should not be called on typed vectors");
                    }
                };
            }
        };
    }
}
