package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import java.util.Iterator;
import java.util.Objects;

import org.apache.commons.lang3.ArrayUtils;
import org.kuse.payloadbuilder.core.parser.Table;

/** Temporary table scan operator */
public class TemporaryTableScanOperator extends AOperator
{
    private final Table table;

    public TemporaryTableScanOperator(int nodeId, Table table)
    {
        super(nodeId);
        this.table = requireNonNull(table, "table");
    }

    @Override
    public String getName()
    {
        return "scan (#" + table.getTable() + ")";
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        TemporaryTable temporaryTable = context.getSession().getTemporaryTable(table.getTable());
        if (table == null)
        {
            throw new OperatorException("Data for temporary table " + table.getTable() + " could not be found");
        }
        final Iterator<TemporaryTable.Row> it = temporaryTable.getRows().iterator();
        return new RowIterator()
        {
            @Override
            public Tuple next()
            {
                return new TemporaryTableTuple(temporaryTable.getColumns(), it.next(), table.getTableAlias().getTupleOrdinal());
            }

            @Override
            public boolean hasNext()
            {
                return it.hasNext();
            }
        };
    }

    @Override
    public int hashCode()
    {
        return table.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TemporaryTableScanOperator)
        {
            TemporaryTableScanOperator that = (TemporaryTableScanOperator) obj;
            return table.getTable().equals(that.table.getTable())
                && table.isTempTable() == that.table.isTempTable()
                && table.getTableAlias().getTupleOrdinal() == that.table.getTableAlias().getTupleOrdinal()
                && Objects.equals(table.getCatalogAlias(), that.table.getCatalogAlias());
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "#" + table.getTable().toString();
    }

    /**
     * <pre>
     * Tuple that wraps tuples from temporary tables and changes it's
     * tuple ordinal depending on the ordinal it got assign when used in queries
     * </pre>
     */
    private static class TemporaryTableTuple implements Tuple
    {
        private final int tupleOrdinal;
        private final Tuple tuple;
        private final String[] columns;
        private final Object[] values;

        private TemporaryTableTuple(String[] columns, TemporaryTable.Row row, int tupleOrdinal)
        {
            this.tuple = row.getTuple();
            this.columns = columns;
            this.values = row.getValues();
            this.tupleOrdinal = tupleOrdinal;
        }

        @Override
        public int getTupleOrdinal()
        {
            return tupleOrdinal;
        }

        @Override
        public Tuple getTuple(int tupleOrdinal)
        {
            if (this.tupleOrdinal == tupleOrdinal)
            {
                return this;
            }

            return null;
        }

        @Override
        public Tuple getSubTuple(int tupleOrdinal)
        {
            return tuple.getTuple(tupleOrdinal);
        }

        @Override
        public int getColumnCount()
        {
            return columns.length;
        }

        @Override
        public int getColumnOrdinal(String column)
        {
            return ArrayUtils.indexOf(columns, column);
        }

        @Override
        public String getColumn(int ordinal)
        {
            return columns[ordinal];
        }

        @Override
        public Object getValue(int ordinal)
        {
            if (ordinal == -1)
            {
                return null;
            }
            return values[ordinal];
        }
    }
}
