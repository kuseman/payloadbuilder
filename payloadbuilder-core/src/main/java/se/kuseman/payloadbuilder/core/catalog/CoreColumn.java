package se.kuseman.payloadbuilder.core.catalog;

import static java.util.Objects.requireNonNull;
import static org.apache.commons.lang3.StringUtils.isBlank;

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

    private CoreColumn(String name, ResolvedType type, MetaData metaData, String outputName, boolean internal, ColumnReference columnReference, Type columnType)
    {
        super(name, type, metaData);
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
        StringBuilder sb = new StringBuilder(super.toString()).append(", columnType=")
                .append(columnType);

        if (internal)
        {
            sb.append(", internal");
        }
        if (!isBlank(outputName))
        {
            sb.append(", outputName=")
                    .append(outputName);

        }
        if (columnReference != null)
        {
            sb.append(", columnReference=")
                    .append(columnReference);
        }

        return sb.toString();
    }

    /** Convert provided column to a {@link CoreColumn} and change it's {@link TableSourceReference}. */
    public static CoreColumn changeTableSource(Column column, TableSourceReference tableSourceReference)
    {
        requireNonNull(tableSourceReference);
        Builder builder = Builder.from(column);
        String columnReferenceName = column.getName();
        TableSourceReference columnTableSourceReference = null;
        if (builder.columnReference != null)
        {
            columnTableSourceReference = builder.columnReference.tableSourceReference();
            columnReferenceName = builder.columnReference.columnName();
        }
        // If there already was a table source connected to input column link it with the provided one
        if (columnTableSourceReference != null)
        {
            tableSourceReference = tableSourceReference.withParent(columnTableSourceReference);
        }
        return builder.withColumnReference(new ColumnReference(columnReferenceName, tableSourceReference, builder.metaData))
                .build();
    }

    /** Construct an asterisk column for provided alias, type and table source. */
    public static CoreColumn asterisk(String alias, ResolvedType type, TableSourceReference tableSourceReference)
    {
        requireNonNull(tableSourceReference);
        ColumnReference cr = new ColumnReference("*", tableSourceReference, Column.MetaData.EMPTY);
        return new CoreColumn(alias, type, MetaData.EMPTY, "", false, cr, Type.ASTERISK);
    }

    /** Core column type */
    public enum Type
    {
        /**
         * An asterisk column which is used in a schema less query for catalogs that doesn't have schemas. This acts as a place holder during planning where the actual columns will come runtime.
         */
        ASTERISK,

        /** A column that is derived from an {@link #ASTERISK} column. This to distinguish between a regular schema and an asterisk schema but with resolved asterisk columns. */
        NAMED_ASTERISK,

        /** A regular column. */
        REGULAR,

        /** A populated column. */
        POPULATED,
    }

    /** Builder. */
    public static class Builder
    {
        private String name;
        private ResolvedType resolvedType;
        private MetaData metaData = MetaData.EMPTY;
        private ColumnReference columnReference;
        private String outputName;
        private boolean internal;
        private Type columnType = Type.REGULAR;

        private Builder(Column column)
        {
            requireNonNull(column);
            this.name = column.getName();
            this.resolvedType = column.getType();
            this.metaData = column.getMetaData();

            if (column instanceof CoreColumn cc)
            {
                this.columnReference = cc.columnReference;
                this.outputName = cc.outputName;
                this.internal = cc.internal;
                this.columnType = cc.columnType;
                if (cc.columnReference != null)
                {
                    this.metaData = cc.columnReference.metaData();
                }
            }
        }

        private Builder(String name, ResolvedType type)
        {
            this.name = requireNonNull(name);
            this.resolvedType = requireNonNull(type);
        }

        public static Builder from(Column column)
        {
            return new Builder(column);
        }

        public static Builder from(String name, ResolvedType type)
        {
            return new Builder(name, type);
        }

        public Builder withName(String newName)
        {
            this.name = requireNonNull(newName);
            return this;
        }

        public Builder withResolvedType(ResolvedType resolvedType)
        {
            this.resolvedType = requireNonNull(resolvedType);
            return this;
        }

        public Builder withMetaData(MetaData metaData)
        {
            this.metaData = requireNonNull(metaData);
            return this;
        }

        // CSOFF
        public Builder withColumnReference(ColumnReference columnReference)
        // CSON
        {
            if (columnReference != null)
            {
                this.metaData = columnReference.metaData();
            }
            this.columnReference = columnReference;
            return this;
        }

        public Builder withOutputName(String outputName)
        {
            this.outputName = outputName;
            return this;
        }

        public Builder withInternal(boolean internal)
        {
            this.internal = internal;
            return this;
        }

        public Builder withColumnType(Type columnType)
        {
            this.columnType = columnType;
            return this;
        }

        public CoreColumn build()
        {
            return new CoreColumn(name, resolvedType, metaData, outputName, internal, columnReference, columnType);
        }
    }
}
