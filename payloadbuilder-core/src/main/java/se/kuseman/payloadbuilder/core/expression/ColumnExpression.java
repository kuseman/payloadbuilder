package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import java.util.List;
import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IColumnExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.StatementContext;

/**
 * Column expression.
 * 
 * <pre>
 * Have one of these forms:
 *   - column:              Fetch value vector from input tuple vector by name
 *   - ordinal:             Fetch value vector from input tuple vector by ordinal
 *   - lambdaId:            Same as column/ordinal but input is taken from lambda id in context instead of input
 * 
 * columnReference:       This acts as a filter when fetching the correct value vector from input tuple vector (only applicable when using non ordinal)
 *   
 * outerReference:        Is set if this column is resolved to the outer schema and then the outer tuple vector is used in context instead of input
 * </pre>
 */
public class ColumnExpression implements IColumnExpression, HasAlias, HasColumnReference
{
    /** Alias for this expression. This is the column name (only used for presentation) */
    private final String alias;

    /** Column reference of expression. */
    private final ColumnReference columnReference;
    /**
     * Flag that indicates that this expression should resolve it's value from the outer reference in context and not by the input. This is a column in a correlated sub query.
     */
    private final boolean outerReference;

    /** Ordinal if this expression could be bound to an ordinal in the input schema. Used when having a static schema. */
    private final int ordinal;

    /** Unbounded column if this column expression must be evaluated reflectively runtime */
    private final String column;

    /** The resolved type for this column expression. */
    private final ResolvedType resolvedType;

    /** Lambda id column. The value resolved will be located in context during runtime */
    private final int lambdaId;

    /** Create a column reference expression. */
    public ColumnExpression(String alias, String column, ResolvedType type, ColumnReference columnReference, int ordinal, boolean outerReference, int lambdaId)
    {
        this.alias = requireNonNull(alias, "alias");
        this.column = column;
        this.resolvedType = requireNonNull(type, "type");
        this.columnReference = columnReference;
        this.outerReference = outerReference;
        this.ordinal = ordinal;
        this.lambdaId = lambdaId;

        // Allowed states:
        // - lambdaId
        // - ordinal
        // - column (with or without tableSource)

        if (ordinal < 0
                && lambdaId < 0
                && column == null)
        {
            throw new IllegalArgumentException("Cannot construct a column expression without either ordinal nor column");
        }
    }

    /** Copy column expression but change the ordinal */
    public ColumnExpression(ColumnExpression source, int ordinal)
    {
        this(source.alias, source.column, source.resolvedType, source.columnReference, ordinal, source.outerReference, source.lambdaId);
    }

    @Override
    public String getColumn()
    {
        return column;
    }

    @Override
    public QualifiedName getQualifiedColumn()
    {
        return QualifiedName.of(alias);
    }

    @Override
    public int getOrdinal()
    {
        return ordinal;
    }

    public boolean isOuterReference()
    {
        return outerReference;
    }

    @Override
    public ColumnReference getColumnReference()
    {
        return columnReference;
    }

    @Override
    public Alias getAlias()
    {
        return new HasAlias.Alias(alias, "");
    }

    @Override
    public ResolvedType getType()
    {
        return resolvedType;
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        TupleVector vector = input;
        if (lambdaId >= 0)
        {
            ValueVector lambdaValue = ((StatementContext) context.getStatementContext()).getLambdaValue(lambdaId);
            if (lambdaValue == null)
            {
                throw new IllegalArgumentException("Expected a lambda value in context for id: " + lambdaId);
            }

            return lambdaValue;
        }
        else if (outerReference)
        {
            vector = ((StatementContext) context.getStatementContext()).getOuterTupleVector();
            if (vector == null)
            {
                throw new IllegalArgumentException("Expected a tuple vector in context when evaluating: " + alias);
            }
        }

        // Ordinal access
        if (ordinal >= 0)
        {
            if (ordinal >= vector.getSchema()
                    .getSize())
            {
                return ValueVector.literalNull(ResolvedType.of(Type.Any), vector.getRowCount());
            }

            return vector.getColumn(ordinal);
        }

        // Else try to find column be schema
        Schema schema = vector.getSchema();
        List<Column> columns = schema.getColumns();
        if (columns.isEmpty())
        {
            throw new IllegalArgumentException("Expected tuple vector to have a schema");
        }

        int indexMatch = -1;
        int size = columns.size();
        for (int i = 0; i < size; i++)
        {
            Column schemaColumn = columns.get(i);
            TableSourceReference columnTableSourceReference = SchemaUtils.getTableSource(schemaColumn);

            // Filter on table source if exists
            // This happens when having an asterisk schema and we only know which table source
            // this columns belongs to and we don't have an ordinal
            if (columnReference != null
                    // Sub query table sources are only a marker during planning and should not be compared
                    && columnReference.tableSourceReference()
                            .getType() != TableSourceReference.Type.SUBQUERY
                    && (columnTableSourceReference == null
                            || columnReference.tableSourceReference()
                                    .getId() != columnTableSourceReference.getId()))
            {
                continue;
            }

            if (StringUtils.equalsIgnoreCase(column, schemaColumn.getName()))
            {
                // TODO: lenient setting that returns first match of column
                if (indexMatch != -1)
                {
                    // Internal columns should not throw
                    if (schemaColumn instanceof CoreColumn
                            && ((CoreColumn) schemaColumn).isInternal())
                    {
                        continue;
                    }

                    throw new QueryException("Ambiguous column: " + column);
                }

                indexMatch = i;
            }
        }

        // TODO: strict mode, throw if not found
        if (indexMatch == -1)
        {
            return ValueVector.literalNull(lambdaId >= 0 ? ResolvedType.array(ResolvedType.of(Type.Any))
                    : ResolvedType.of(Type.Any), vector.getRowCount());
        }

        return vector.getColumn(indexMatch);
    }

    @Override
    public int hashCode()
    {
        return alias.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj == null)
        {
            return false;
        }
        else if (obj == this)
        {
            return true;
        }
        else if (obj instanceof ColumnExpression that)
        {
            return alias.equals(that.alias)
                    && Objects.equals(column, that.column)
                    && Objects.equals(columnReference, that.columnReference)
                    && ordinal == that.ordinal
                    && outerReference == that.outerReference
                    && lambdaId == that.lambdaId
                    && resolvedType.equals(that.resolvedType);
        }
        return false;
    }

    @Override
    public String toString()
    {
        String tableSourceAlias = columnReference != null ? columnReference.tableSourceReference()
                .getAlias()
                : "";

        return (!"".equals(tableSourceAlias) ? (tableSourceAlias + ".")
                : "")
                + (column == null ? alias
                        : column);
    }

    @Override
    public String toVerboseString()
    {
        // column (ordinal=X, outer=true, table=Y, lambda=Z)

        StringBuilder sb = new StringBuilder();
        sb.append((column == null ? alias
                : column));

        boolean hasOptions = ordinal >= 0
                || lambdaId >= 0
                || outerReference
                || columnReference != null;
        if (hasOptions)
        {
            sb.append(" (");
        }

        if (ordinal >= 0)
        {
            sb.append("ordinal=")
                    .append(ordinal);
        }
        if (lambdaId >= 0)
        {
            sb.append(" lambda=")
                    .append(lambdaId);
        }
        if (outerReference)
        {
            sb.append(" outer=true");
        }
        if (columnReference != null)
        {
            sb.append(" table=")
                    .append(columnReference.tableSourceReference())
                    .append(", columnType=")
                    .append(columnReference.columnType());
        }

        if (hasOptions)
        {
            sb.append(")");
        }
        return sb.toString();
    }

    /** Column expression builder */
    public static class Builder
    {
        private final String alias;
        private final ResolvedType resolvedType;
        private String column;
        private ColumnReference columnReference;
        private boolean outerReference;
        private int ordinal = -1;
        private int lambdaId = -1;

        private Builder(String alias, ResolvedType type)
        {
            this.alias = requireNonNull(alias, "alias");
            this.resolvedType = requireNonNull(type, "resolvedType");
        }

        public static Builder of(String alias, ResolvedType type)
        {
            return new Builder(alias, type);
        }

        public Builder withColumn(String column)
        {
            this.column = column;
            return this;
        }

        public Builder withOuterReference(boolean outerReference)
        {
            this.outerReference = outerReference;
            return this;
        }

        public Builder withOrdinal(int ordinal)
        {
            this.ordinal = ordinal;
            return this;
        }

        public Builder withColumnReference(ColumnReference columnReference)
        {
            this.columnReference = columnReference;
            return this;
        }

        public Builder withLambdaId(int lambdaId)
        {
            this.lambdaId = lambdaId;
            return this;
        }

        public ColumnExpression build()
        {
            return new ColumnExpression(alias, column, resolvedType, columnReference, ordinal, outerReference, lambdaId);
        }
    }
}
