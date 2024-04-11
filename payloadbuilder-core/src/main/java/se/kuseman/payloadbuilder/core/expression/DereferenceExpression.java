package se.kuseman.payloadbuilder.core.expression;

import static java.util.Objects.requireNonNull;

import java.util.Objects;

import org.apache.commons.lang3.tuple.Pair;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectVector;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.execution.vector.IValueVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IDereferenceExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SchemaUtils;
import se.kuseman.payloadbuilder.core.execution.VectorUtils;
import se.kuseman.payloadbuilder.core.parser.Location;
import se.kuseman.payloadbuilder.core.parser.ParseException;

/** Dereference expression. expression.column reference */
public class DereferenceExpression implements IDereferenceExpression, HasAlias, HasTableSourceReference
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
    public TableSourceReference getTableSourceReference()
    {
        if (left instanceof HasTableSourceReference htsr)
        {
            return htsr.getTableSourceReference();
        }
        return null;
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

        final int rowCount = input.getRowCount();
        ValueVector leftResult = left.eval(input, context);

        if (leftResult.type()
                .getType() == Type.Table)
        {
            IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                    .getObjectVectorBuilder(resolvedType, rowCount);

            for (int i = 0; i < rowCount; i++)
            {
                if (leftResult.isNull(i))
                {
                    builder.putNull();
                }
                else
                {
                    TupleVector vector = leftResult.getTable(i);

                    int ordinal = DereferenceExpression.this.ordinal;
                    if (ordinal < 0)
                    {
                        ordinal = getOrdinalInternal(vector.getSchema(), right, null, false).getValue();
                        if (ordinal < 0)
                        {
                            builder.put(ValueVector.literalNull(ResolvedType.of(Type.Any), 0));
                            continue;
                        }
                    }

                    builder.put(vector.getColumn(ordinal));
                }
            }
            return builder.build();
        }
        else if (leftResult.type()
                .getType() == Type.Object)
        {
            IValueVectorBuilder builder = context.getVectorBuilderFactory()
                    .getValueVectorBuilder(resolvedType, rowCount);

            Schema schema = leftResult.type()
                    .getSchema();

            for (int i = 0; i < rowCount; i++)
            {
                if (leftResult.isNull(i))
                {
                    builder.putNull();
                }
                else
                {
                    int ordinal = this.ordinal;
                    if (ordinal < 0)
                    {
                        // Asterisk schema, resolve
                        Pair<Column, Integer> pair = getOrdinalInternal(schema, right, null, false);
                        if (pair.getKey() == null)
                        {
                            builder.putNull();
                            continue;
                        }
                        ordinal = pair.getValue();
                    }

                    ObjectVector object = leftResult.getObject(i);
                    ValueVector value = object.getValue(ordinal);
                    builder.put(value, object.getRow());
                }
            }
            return builder.build();
        }

        IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                .getObjectVectorBuilder(resolvedType, rowCount);

        for (int i = 0; i < rowCount; i++)
        {
            if (leftResult.isNull(i))
            {
                builder.putNull();
            }
            else
            {
                Object value = VectorUtils.convert(leftResult.getAny(i));
                if (value instanceof ObjectVector)
                {
                    ObjectVector object = (ObjectVector) value;
                    Pair<Column, Integer> pair = getOrdinalInternal(object.getSchema(), right, null, false);
                    if (pair.getKey() == null)
                    {
                        builder.putNull();
                    }
                    else
                    {
                        ValueVector objectValue = object.getValue(pair.getValue());
                        builder.put(objectValue, object.getRow());
                    }
                }
                else
                {
                    throw new IllegalArgumentException("Cannot de-reference '" + right + "' from: " + value);
                }
            }
        }

        return builder.build();
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    private static Pair<Column, Integer> getOrdinalInternal(Schema schema, String right, Location location, boolean throwIfNotFound)
    {
        boolean asteriskInSchema = false;
        int size = schema.getSize();
        Column match = null;
        int ordinal = -1;
        for (int i = 0; i < size; i++)
        {
            Column column = schema.getColumns()
                    .get(i);

            asteriskInSchema = asteriskInSchema
                    || SchemaUtils.isAsterisk(column);

            if (right.equalsIgnoreCase(column.getName()))
            {
                match = column;
                ordinal = i;
                break;
            }
        }

        if (!asteriskInSchema
                && match == null
                && throwIfNotFound)
        {
            throw new ParseException("No column found in object named: " + right + ", expected one of: " + schema.getColumns(), location);
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
    public static IExpression create(IExpression expression, QualifiedName qname, Location location)
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
            if (type.getType() == Type.Table)
            {
                Pair<Column, Integer> pair = getOrdinalInternal(type.getSchema(), part, location, true);
                ordinal = pair.getValue();
                if (pair.getKey() != null)
                {
                    resolvedType = ResolvedType.array(pair.getKey()
                            .getType());
                }
                else
                {
                    resolvedType = ResolvedType.array(resolvedType);
                }
            }
            else if (type.getType() == Type.Object)
            {
                Pair<Column, Integer> pair = getOrdinalInternal(type.getSchema(), part, location, true);
                ordinal = pair.getValue();
                if (pair.getKey() != null)
                {
                    resolvedType = pair.getKey()
                            .getType();
                }
            }

            result = new DereferenceExpression(result, part, ordinal, resolvedType);
        }

        return result;
    }
}
