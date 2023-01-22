package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import java.util.Map;
import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ColumnReference;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IDereferenceExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

/** Dereference expression. expression.column reference */
public class DereferenceExpression implements IDereferenceExpression, HasAlias
{
    private final IExpression left;
    private final String right;
    private final int ordinal;
    private final ResolvedType resolvedType;

    /** Unresolved ctor */
    public DereferenceExpression(IExpression left, String right)
    {
        this.left = requireNonNull(left, "left");
        this.right = requireNonNull(right, "right");
        this.ordinal = -1;
        this.resolvedType = null;
    }

    /** Resolved ctor */
    public DereferenceExpression(IExpression left, String right, int ordinal, ResolvedType resolvedType)
    {
        this.left = requireNonNull(left, "left");
        this.right = requireNonNull(right, "right");
        this.ordinal = ordinal;
        this.resolvedType = requireNonNull(resolvedType);
    }

    @Override
    public IExpression getExpression()
    {
        return left;
    }

    @Override
    public String getRight()
    {
        return right;
    }

    public int getOrdinal()
    {
        return ordinal;
    }

    public boolean isResolved()
    {
        return resolvedType != null;
    }

    @Override
    public Alias getAlias()
    {
        return new Alias(right, "");
    }

    @Override
    public ColumnReference getColumnReference()
    {
        return left.getColumnReference();
    }

    @Override
    public QualifiedName getQualifiedColumn()
    {
        QualifiedName qname = left.getQualifiedColumn();
        if (qname != null)
        {
            // Combine the qname with right
            return qname.extend(right);
        }
        return null;
    }

    @Override
    public ResolvedType getType()
    {
        if (resolvedType == null)
        {
            throw new IllegalArgumentException("An un-resolved dereference expression has no type");
        }

        return resolvedType;
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        if (resolvedType == null)
        {
            throw new IllegalArgumentException("An un-resolved dereference expression cannot be evaluated");
        }

        ValueVector leftResult = left.eval(input, context);

        // ValueVector result, extract tuple vector and return column
        if (resolvedType.getType() == Type.ValueVector)
        {
            if (leftResult.type()
                    .getType() != Type.TupleVector)
            {
                throw new IllegalArgumentException("Expected a tupe vector type in result but got: " + leftResult.type());
            }
            return new ValueVector()
            {
                @Override
                public ResolvedType type()
                {
                    return resolvedType;
                }

                @Override
                public int size()
                {
                    return leftResult.size();
                }

                @Override
                public boolean isNull(int row)
                {
                    return leftResult.isNull(row);
                }

                @Override
                public Object getValue(int row)
                {
                    TupleVector vector = (TupleVector) leftResult.getValue(row);

                    int ordinal = DereferenceExpression.this.ordinal;
                    if (ordinal < 0)
                    {
                        ordinal = getOrdinalInternal(vector.getSchema(), right).getValue();
                        if (ordinal < 0)
                        {
                            return ValueVector.literalNull(ResolvedType.of(Type.Any), 0);
                        }
                    }

                    return vector.getColumn(ordinal);
                }
            };
        }

        // Unknown result, PLB only supports Map's at the moment
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return resolvedType;
            }

            @Override
            public int size()
            {
                return leftResult.size();
            }

            @Override
            public boolean isNull(int row)
            {
                return getValue(row) == null;
            }

            @SuppressWarnings("unchecked")
            @Override
            public Object getValue(int row)
            {
                Object value = leftResult.getValue(row);
                if (value == null)
                {
                    return null;
                }
                else if (value instanceof Map)
                {
                    return ((Map<String, Object>) value).get(right);
                }

                throw new IllegalArgumentException("Cannot de-reference '" + right + "' from: " + value);
            }
        };
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    private static Pair<Column, Integer> getOrdinalInternal(Schema schema, String right)
    {
        boolean asteriskInSchema = false;
        int size = schema.getSize();
        Column match = null;
        int ordinal = -1;
        for (int i = 0; i < size; i++)
        {
            Column column = schema.getColumns()
                    .get(i);

            ColumnReference colRef = column.getColumnReference();
            asteriskInSchema = asteriskInSchema
                    || (colRef != null
                            && colRef.isAsterisk());

            if (right.equalsIgnoreCase(column.getName()))
            {
                match = column;
                ordinal = i;
                break;
            }
        }

        return Pair.of(match, asteriskInSchema ? -1
                : ordinal);
    }

    @Override
    public int hashCode()
    {
        // CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + left.hashCode();
        hashCode = hashCode * 37 + right.hashCode();
        return hashCode;
        // CSON
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
        else if (obj instanceof DereferenceExpression)
        {
            DereferenceExpression that = (DereferenceExpression) obj;
            return left.equals(that.left)
                    && right.equals(that.right)
                    && Objects.equals(resolvedType, that.resolvedType)
                    && ordinal == that.ordinal;
        }
        return false;
    }

    @Override
    public String toString()
    {
        return left.toString() + "." + right;
    }

    @Override
    public String toVerboseString()
    {
        return "[" + left.toVerboseString()
               + "]"
               + "."
               + right
               + (ordinal >= 0 ? " (" + ordinal + ")"
                       : "");
    }

    /** Create a resolved dereference expression. NOTE! Requires that expression is resolved */
    public static IExpression create(IExpression expression, QualifiedName qname)
    {
        IExpression result = expression;
        // Create nested dereference for all parts in qname
        int size = qname.size();
        for (int i = 0; i < size; i++)
        {
            String part = qname.getParts()
                    .get(i);

            int ordinal = -1;
            ResolvedType resolvedType = ResolvedType.of(Type.Any);

            // Resolve this de-reference ordinal and type
            ResolvedType type = result.getType();
            if (type.getType() == Type.TupleVector)
            {
                Pair<Column, Integer> pair = getOrdinalInternal(type.getSchema(), part);
                ordinal = pair.getValue();
                if (pair.getKey() != null)
                {
                    resolvedType = ResolvedType.valueVector(pair.getKey()
                            .getType());
                }
                else
                {
                    resolvedType = ResolvedType.valueVector(resolvedType);
                }
            }

            result = new DereferenceExpression(result, part, ordinal, resolvedType);
        }

        return result;
    }
}
