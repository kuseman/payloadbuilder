package se.kuseman.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;

/** Extension of {@link Column} that attaches different properties used during planning and execution */
public class CoreColumn extends Column
{
    /**
     * Set if this column references a table source. Used when resolving asterisk schemas etc. To connect a column to a specific table source.
     */
    private final TableSourceReference tableSourceReference;

    /** Type of column */
    private final Type columnType;

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
    /** Internal column that should not be used as output */
    private final boolean internal;

    public CoreColumn(String name, ResolvedType type, String outputName, boolean internal)
    {
        this(name, type, outputName, internal, null, Type.REGULAR);
    }

    /** Copy ctor. New type of column */
    public CoreColumn(CoreColumn source, ResolvedType type, TableSourceReference tableSource)
    {
        this(source.getName(), type, source.outputName, source.internal, tableSource, source.columnType);
    }

    public CoreColumn(String name, ResolvedType type, String outputName, boolean internal, TableSourceReference tableSourceReference, Type columnType)
    {
        super(name, type);
        this.outputName = Objects.toString(outputName, "");
        this.internal = internal;
        this.tableSourceReference = tableSourceReference;
        this.columnType = requireNonNull(columnType, "columnType");
    }

    public TableSourceReference getTableSourceReference()
    {
        return tableSourceReference;
    }

    public Type getColumnType()
    {
        return columnType;
    }

    /** Return the output name of this column */
    public String getOutputName()
    {
        return !StringUtils.isBlank(outputName) ? outputName
                : getName();
    }

    public boolean isInternal()
    {
        return internal;
    }

    @Override
    public int hashCode()
    {
        return super.hashCode();
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
        else if (obj instanceof CoreColumn that)
        {
            return super.equals(that)
                    && Objects.equals(outputName, that.outputName)
                    && internal == that.internal
                    && Objects.equals(tableSourceReference, that.tableSourceReference)
                    && columnType == that.columnType;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return getName() + " ("
               + (!StringUtils.isBlank(outputName) ? outputName + " "
                       : "")
               + getType()
               + ")";
    }

    /** Construct a {@link CoreColumn} from name and type */
    public static CoreColumn of(String name, ResolvedType type)
    {
        return new CoreColumn(name, type, "", false, null, Type.REGULAR);
    }

    /** Return a {@link CoreColumn} with provided name and type and reference */
    public static CoreColumn of(String name, ResolvedType type, TableSourceReference tableSource)
    {
        return new CoreColumn(name, type, "", false, tableSource, Type.REGULAR);
    }

    /** Reference column type */
    public enum Type
    {
        /**
         * An asterisk column which is used in a schema less query for catalogs that doesn't have schemas. This acts as a place holder during planning where the actual columns will come runtime.
         */
        ASTERISK,

        /** A regular column. */
        REGULAR,

        /** A populated column. */
        POPULATED,
    }
}
