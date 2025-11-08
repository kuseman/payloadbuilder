package se.kuseman.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;

/** Extension of {@link Column} that attaches different properties used during planning and execution */
public class CoreColumn extends Column
{
    /** Column reference if this column points to one. */
    private final ColumnReference columnReference;

    /** Type of column */
    private final Type columnType;

    /**
     * Name of the column when written to output. Some columns are generated during planning and have a auto generated schema name ({@link Column#getName()}) but a different name when written to
     * output.
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

    public CoreColumn(String name, ResolvedType type, String outputName, boolean internal, ColumnReference columnReference, Type columnType)
    {
        super(name, type);
        this.outputName = Objects.toString(outputName, "");
        this.internal = internal;
        this.columnReference = columnReference;
        this.columnType = requireNonNull(columnType, "columnType");
    }

    public ColumnReference getColumnReference()
    {
        return columnReference;
    }

    public Type getColumnType()
    {
        return columnType;
    }

    public TableSourceReference getTableSourceReference()
    {
        return columnReference != null ? columnReference.tableSourceReference()
                : null;
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
                    && Objects.equals(columnReference, that.columnReference)
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
        return of(name, type, null);
    }

    /** Construct a {@link CoreColumn} from name and type */
    public static CoreColumn of(String name, ResolvedType type, TableSourceReference tableSourceReference)
    {
        return of(name, type, tableSourceReference, Type.REGULAR);
    }

    /** Construct a {@link CoreColumn} from name and type */
    public static CoreColumn of(String name, ResolvedType type, TableSourceReference tableSourceReference, Type columnType)
    {
        ColumnReference cr = null;
        if (tableSourceReference != null)
        {
            cr = new ColumnReference(name, tableSourceReference);
        }
        return new CoreColumn(name, type, "", false, cr, columnType);
    }

    /** Convert provided column to a {@link CoreColumn} and change properties. */
    public static CoreColumn changeProperties(Column column, TableSourceReference tableSourceReference)
    {
        return changeProperties(column, column.getType(), tableSourceReference);
    }

    /** Convert provided column to a {@link CoreColumn} and change properties. */
    public static CoreColumn changeProperties(Column column, ResolvedType type)
    {
        return changeProperties(column, type, null);
    }

    /** Convert provided column to a {@link CoreColumn} and change properties. */
    public static CoreColumn changeProperties(Column column, ResolvedType type, TableSourceReference tableSourceReference)
    {
        boolean internal = false;
        String outputName = "";
        Type columnType = Type.REGULAR;
        String columnReferenceName = column.getName();
        TableSourceReference columnTableSourceReference = null;
        if (column instanceof CoreColumn cc)
        {
            internal = cc.internal;
            outputName = cc.outputName;
            columnType = cc.columnType;
            if (cc.columnReference != null)
            {
                columnReferenceName = cc.columnReference.columnName();
                columnTableSourceReference = cc.columnReference.tableSourceReference();
            }
        }
        ColumnReference cr = null;
        // Use the columns table source reference if input is null
        if (tableSourceReference == null)
        {
            tableSourceReference = columnTableSourceReference;
        }
        // Link the columns table source reference with the input
        else if (columnTableSourceReference != null)
        {
            tableSourceReference = tableSourceReference.withParent(columnTableSourceReference);
        }

        if (tableSourceReference != null)
        {
            cr = new ColumnReference(columnReferenceName, tableSourceReference);
        }
        return new CoreColumn(column.getName(), type, outputName, internal, cr, columnType);
    }

    /** Construct an asterisk column for provided alias and table source. */
    public static CoreColumn asterisk(String alias, TableSourceReference tableSourceRefernece)
    {
        return asterisk(alias, ResolvedType.ANY, tableSourceRefernece);
    }

    /** Construct an asterisk column for provided alias and table source. */
    public static CoreColumn asterisk(String alias, ResolvedType type, TableSourceReference tableSourceRefernece)
    {
        ColumnReference cr = null;
        if (tableSourceRefernece != null)
        {
            cr = new ColumnReference("*", tableSourceRefernece);
        }
        return new CoreColumn(alias, type, "", false, cr, Type.ASTERISK);
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
