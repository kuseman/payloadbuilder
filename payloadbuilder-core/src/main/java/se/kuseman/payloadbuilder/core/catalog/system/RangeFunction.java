package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.TableMeta;
import se.kuseman.payloadbuilder.api.TableMeta.DataType;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.api.operator.Operator.TupleIterator;
import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Range table valued function that emits row in range */
class RangeFunction extends TableFunctionInfo
{
    private static final TableMeta TABLE_META = new TableMeta(asList(new TableMeta.Column("Value", DataType.INT)));

    RangeFunction(Catalog catalog)
    {
        super(catalog, "range");
    }

    @Override
    public TableMeta getTableMeta()
    {
        return TABLE_META;
    }

    @Override
    public TupleIterator open(IExecutionContext context, String catalogAlias, TableAlias tableAlias, List<? extends IExpression> arguments)
    {
        int from = 0;
        int to = -1;
        // to
        if (arguments.size() <= 1)
        {
            to = ((Number) requireNonNull(arguments.get(0)
                    .eval(context), "From argument to range cannot be null.")).intValue();
        }
        // from, to
        else if (arguments.size() <= 2)
        {
            from = ((Number) requireNonNull(arguments.get(0)
                    .eval(context), "From argument to range cannot be null.")).intValue();
            to = ((Number) requireNonNull(arguments.get(1)
                    .eval(context), "To argument to range cannot be null.")).intValue();
        }

        final int start = from;
        final int stop = to;

        // CSOFF
        return new TupleIterator()
        // CSON
        {
            int pos = start;

            @Override
            public Tuple next()
            {
                return new RangeTuple(pos++, tableAlias.getTupleOrdinal());
            }

            @Override
            public boolean hasNext()
            {
                return pos < stop;
            }
        };
    }

    /** Tuple */
    private static class RangeTuple implements Tuple
    {
        private final int value;
        private final int tupleOrdinal;

        private RangeTuple(int value, int tupleOrdinal)
        {
            this.value = value;
            this.tupleOrdinal = tupleOrdinal;
        }

        @Override
        public int getTupleOrdinal()
        {
            return tupleOrdinal;
        }

        @Override
        public Object getValue(int columnOrdinal)
        {
            if (columnOrdinal == 0)
            {
                return value;
            }

            return null;
        }

        @Override
        public int getInt(int columnOrdinal)
        {
            if (columnOrdinal == 0)
            {
                return value;
            }
            return 0;
        }

        @Override
        public boolean isNull(int columnOrdinal)
        {
            if (columnOrdinal == 0)
            {
                return false;
            }

            return true;
        }

        @Override
        public int getColumnCount()
        {
            return 1;
        }

        @Override
        public String getColumn(int columnOrdinal)
        {
            if (columnOrdinal == 0)
            {
                return "Value";
            }
            return null;
        }

        @Override
        public int getColumnOrdinal(String column)
        {
            if ("Value".equals(column))
            {
                return 0;
            }
            return -1;
        }
    }
}
