package com.viskan.payloadbuilder;

import static java.util.Objects.requireNonNull;

/** Table object (alias and column) */
public class TableObject
{
    private final TableAlias alias;
    private final String column;

    public TableObject(TableAlias alias, String column)
    {
        this.alias = requireNonNull(alias, "ailas");
        this.column = requireNonNull(column, "column");
    }

    public TableAlias getAlias()
    {
        return alias;
    }

    public String getColumn()
    {
        return column;
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * (alias.getTable().hashCode() + alias.getAlias().hashCode()) +
            37 * column.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TableObject)
        {
            TableObject that = (TableObject) obj;
            return alias.isEqual(that.alias)
                && column.equals(that.column);
        }
        return false;
    }
}
