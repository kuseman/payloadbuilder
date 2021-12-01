package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.catalog.TableMeta;
import org.kuse.payloadbuilder.core.catalog.TableMeta.DataType;
import org.kuse.payloadbuilder.core.operator.ExecutionContext;
import org.kuse.payloadbuilder.core.operator.Operator.TupleIterator;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.operator.Tuple;
import org.kuse.payloadbuilder.core.parser.Expression;

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
    public TupleIterator open(ExecutionContext context, String catalogAlias, TableAlias tableAlias, List<Expression> arguments)
    {
        int from = 0;
        int to = -1;
        // to
        if (arguments.size() <= 1)
        {
            to = ((Number) requireNonNull(arguments.get(0).eval(context), "From argument to range cannot be null.")).intValue();
        }
        // from, to
        else if (arguments.size() <= 2)
        {
            from = ((Number) requireNonNull(arguments.get(0).eval(context), "From argument to range cannot be null.")).intValue();
            to = ((Number) requireNonNull(arguments.get(1).eval(context), "To argument to range cannot be null.")).intValue();
        }

        final int start = from;
        final int stop = to;

        //CSOFF
        return new TupleIterator()
        //CSON
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
