package org.kuse.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;

import java.util.List;

/** Definition of a tables meta data. Columns data types etc. */
public class TableMeta
{
    private final List<Column> columns;

    public TableMeta(List<Column> columns)
    {
        this.columns = requireNonNull(columns, "columns");
        if (columns.isEmpty())
        {
            throw new IllegalArgumentException("Columns cannot be empty");
        }
        int size = columns.size();
        for (int i = 0; i < size; i++)
        {
            columns.get(i).ordinal = i;
        }
    }

    /** Get column by name */
    public Column getColumn(String name)
    {
        int size = columns.size();
        for (int i = 0; i < size; i++)
        {
            Column column = columns.get(i);
            if (equalsIgnoreCase(name, column.name))
            {
                return column;
            }
        }
        return null;
    }

    /** Get column by ordinal */
    public Column getColumn(int ordinal)
    {
        return columns.get(ordinal);
    }

    public List<Column> getColumns()
    {
        return columns;
    }

    @Override
    public int hashCode()
    {
        return columns.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TableMeta)
        {
            TableMeta that = (TableMeta) obj;
            return columns.equals(that.columns);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return columns.stream().map(c -> c.getName() + "#" + c.getType()).collect(joining(", "));
    }

    /** Column */
    public static class Column
    {
        private final String name;
        private final DataType type;
        private int ordinal;

        public Column(String name, DataType type)
        {
            this.name = requireNonNull(name, "name");
            this.type = requireNonNull(type, "type");
        }

        public int getOrdinal()
        {
            return ordinal;
        }

        public String getName()
        {
            return name;
        }

        public DataType getType()
        {
            return type;
        }

        @Override
        public int hashCode()
        {
            return name.hashCode();
        }

        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof Column)
            {
                Column that = (Column) obj;
                return name.equals(that.name)
                    && type == that.type;
            }
            return false;
        }
    }

    /** Column type */
    public enum DataType
    {
        INT(0),
        LONG(1),
        FLOAT(2),
        DOUBLE(3),
        BOOLEAN(-1),
        /** Any object. Will use reflection when using code gen etc. */
        ANY(-1);

        final int promotePrio;

        DataType(int promotePrio)
        {
            this.promotePrio = promotePrio;
        }

        /** Returns true if this data type is a number */
        public boolean isNumber()
        {
            return promotePrio != -1;
        }

        /** Return the type with highest promote prio. */
        public DataType promote(DataType type)
        {
            if (promotePrio == -1 || type.promotePrio == -1)
            {
                return null;
            }

            return promotePrio > type.promotePrio ? this : type;
        }

        /** Return the java type string for this data type */
        public String getJavaTypeString()
        {
            switch (this)
            {
                case ANY:
                    return "Object";
                case BOOLEAN:
                    return "boolean";
                case DOUBLE:
                    return "double";
                case FLOAT:
                    return "float";
                case INT:
                    return "int";
                case LONG:
                    return "long";
                default:
                    throw new IllegalArgumentException("Unkown type " + this);
            }
        }

        /** Get default value string for this type */
        public String getJavaDefaultValue()
        {
            switch (this)
            {
                case ANY:
                    return "null";
                case BOOLEAN:
                    return "false";
                case DOUBLE:
                    return "0.0";
                case FLOAT:
                    return "0.0f";
                case INT:
                    return "0";
                case LONG:
                    return "0l";
                default:
                    throw new IllegalArgumentException("Unkown type " + this);
            }
        }
    }
}
