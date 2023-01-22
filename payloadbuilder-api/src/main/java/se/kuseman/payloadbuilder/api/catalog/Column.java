package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import se.kuseman.payloadbuilder.api.OutputWriter;
import se.kuseman.payloadbuilder.api.utils.StringUtils;

/** A column of a schema */
public class Column
{
    private final String name;
    /**
     * Name of the column when written to output. Some columns are generated during planning and have a auto generated schema name ({@link #name} but a different name when written to output.
     * 
     * <pre>
     * For example
     * 
     * select max(col1 + col2)
     * from table
     * group by col
     * order by max(col1 + col2)
     * 
     * Here "max(col1 + col2)" will be pushed down with a given name like __expr0
     * to avoid double calculations
     * but the output name will be "max(col1 + col2)"
     * 
     * </pre>
     */
    private final String outputName;
    private final ResolvedType type;
    /** Column reference if column is a direct reference to a column. */
    private final ColumnReference columnReference;

    /** Internal column that should not be used as output */
    private final boolean internal;

    /** Construct a column with type */
    public Column(String name, ResolvedType type)
    {
        this(name, type, null);
    }

    /** Construct a column with a type and column reference */
    public Column(String name, ResolvedType type, ColumnReference columnReference)
    {
        this(name, type, columnReference, false);
    }

    /** Construct a column with a type and column reference */
    public Column(String name, ResolvedType type, ColumnReference columnReference, boolean internal)
    {
        this(name, "", type, columnReference, internal);
    }

    /** Construct a column with a type and column reference */
    public Column(String name, String outputName, ResolvedType type, ColumnReference columnReference, boolean internal)
    {
        this.name = requireNonNull(name, "name");
        this.type = requireNonNull(type, "type");
        this.columnReference = columnReference;
        this.internal = internal;
        this.outputName = requireNonNull(outputName, "outputName");
    }

    /** Recreate column and attach a table source to it resulting in a column reference */
    public Column(Column column, TableSourceReference tableSource)
    {
        this(column.getName(), column.getType(), tableSource.column(column.getName()));
    }

    public String getName()
    {
        return name;
    }

    /** Return the output name of this column */
    public String getOutputName()
    {
        return !StringUtils.isBlank(outputName) ? outputName
                : name;
    }

    public ResolvedType getType()
    {
        return type;
    }

    public ColumnReference getColumnReference()
    {
        return columnReference;
    }

    public boolean isInternal()
    {
        return internal;
    }

    public static Column of(String name, ResolvedType type)
    {
        return new Column(name, type);
    }

    public static Column of(String name, Type type)
    {
        return new Column(name, ResolvedType.of(type));
    }

    public static Column of(ColumnReference reference, ResolvedType type)
    {
        return new Column(reference.getName(), type, reference);
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == this)
        {
            return true;
        }
        else if (obj == null)
        {
            return false;
        }
        else if (obj instanceof Column)
        {
            Column that = (Column) obj;
            return Objects.equals(columnReference, that.columnReference)
                    && name.equals(that.name)
                    && Objects.equals(outputName, that.outputName)
                    && type.equals(that.type)
                    && internal == that.internal;
        }
        return false;
    }

    @Override
    public String toString()
    {
        if (columnReference != null)
        {
            return columnReference + (!"".equals(name) ? "(" + name + ")"
                    : "")
                   + " ("
                   + type
                   + ")";
        }

        return name + " ("
               + (!StringUtils.isBlank(outputName) ? outputName + " "
                       : "")
               + type
               + ")";
    }

    /** Data type of column */
    public enum Type
    {
        /** Unkown type. Can be arbitrary value that is used reflectively runtime */
        Any(0, false, false),
        String(30, false, false),
        Boolean(40, false, false),
        Int(50, true, false),
        Long(60, true, false),
        Float(70, true, false),
        Double(80, true, false),

        DateTime(90, false, false),

        /** A value that can be written using {@link OutputWriter} */
        OutputWritable(1000, false, true),
        /** Nested table. Result from a populated join */
        TupleVector(2000, false, true),
        /** Each row contains a nested value vector */
        ValueVector(3000, false, false);

        private final boolean number;
        private final boolean complex;

        private final int precedence;

        Type(int precedence, boolean number, boolean complex)
        {
            this.precedence = precedence;
            this.number = number;
            this.complex = complex;
        }

        public boolean isNumber()
        {
            return number;
        }

        public boolean isComplex()
        {
            return complex;
        }

        public int getPrecedence()
        {
            return precedence;
        }
    }
}
