package se.kuseman.payloadbuilder.core.catalog;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;

/** Extension of {@link Column} that attaches different properties used during planning and execution */
public class CoreColumn extends Column
{
    /** Column reference if column is a direct reference to a column. */
    private final ColumnReference columnReference;

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
        this(name, type, outputName, internal, null);
    }

    public CoreColumn(String name, ResolvedType type, String outputName, boolean internal, ColumnReference columnReference)
    {
        super(name, type);
        this.outputName = outputName;
        this.internal = internal;
        this.columnReference = columnReference;
    }

    /** Recreate column and attach a table source to it resulting in a column reference */
    public CoreColumn(Column column, TableSourceReference tableSource)
    {
        this(column.getName(), column.getType(), "", false, tableSource.column(column.getName()));
    }

    /** Construct a column with a type and column reference */
    public CoreColumn(String name, ResolvedType type, ColumnReference columnReference)
    {
        this(name, type, "", false, columnReference);
    }

    public ColumnReference getColumnReference()
    {
        return columnReference;
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
        else if (obj instanceof CoreColumn)
        {
            CoreColumn that = (CoreColumn) obj;
            return super.equals(that)
                    && Objects.equals(outputName, that.outputName)
                    && internal == that.internal
                    && Objects.equals(columnReference, that.columnReference);
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
        return new CoreColumn(name, type, null);
    }

    /** Construct a {@link CoreColumn} from name and type */
    public static CoreColumn of(String name, Type type)
    {
        return new CoreColumn(name, ResolvedType.of(type), null);
    }

    /** Construct a {@link CoreColumn} from a reference and a type */
    public static CoreColumn of(ColumnReference reference, ResolvedType type)
    {
        return new CoreColumn(reference.getName(), type, reference);
    }

    /** Return a {@link CoreColumn} with provided name and type and reference */
    public static CoreColumn of(String name, ResolvedType type, ColumnReference colRef)
    {
        return new CoreColumn(name, type, "", false, colRef);
    }

    /** Return a {@link CoreColumn} */
    public static CoreColumn of(String name, ResolvedType type, String outputName, boolean internal, ColumnReference colRef)
    {
        return new CoreColumn(name, type, outputName, internal, colRef);
    }
}
