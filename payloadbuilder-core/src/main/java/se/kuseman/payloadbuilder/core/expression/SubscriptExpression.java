package se.kuseman.payloadbuilder.core.expression;

import static java.util.Arrays.asList;
import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.catalog.ValueVectorAdapter;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.ISubscriptExpression;

/** Subscript. index acces etc. ie "array[0]" */
public class SubscriptExpression implements ISubscriptExpression, HasAlias
{
    private final IExpression value;
    private final IExpression subscript;

    public SubscriptExpression(IExpression value, IExpression subscript)
    {
        this.value = requireNonNull(value, "value");
        this.subscript = requireNonNull(subscript, "subscript");
    }

    @Override
    public IExpression getValue()
    {
        return value;
    }

    @Override
    public IExpression getSubscript()
    {
        return subscript;
    }

    @Override
    public List<IExpression> getChildren()
    {
        return asList(value, subscript);
    }

    @Override
    public Alias getAlias()
    {
        if (value instanceof HasAlias)
        {
            return ((HasAlias) value).getAlias();
        }
        return null;
    }

    @Override
    public ResolvedType getType()
    {
        ResolvedType type = value.getType();
        ResolvedType subScriptType = subscript.getType();

        if (type.getType() == Type.TupleVector)
        {
            // Subscript must be either integer/string/object
            if (!(subScriptType.getType() == Type.Int
                    || subScriptType.getType() == Type.String
                    || subScriptType.getType() == Type.Any))
            {
                throw new IllegalArgumentException("Cannot subscript a TupleVector with " + subScriptType.getType());
            }

            // Filtered row in tuple vector
            if (subScriptType.getType() == Type.Int)
            {
                return type;
            }
            // Column
            else if (subScriptType.getType() == Type.String)
            {
                // TODO: Can we detect type, how to find column ?
                return ResolvedType.valueVector(ResolvedType.of(Type.Any));
            }

            // Not known until runtime
            return ResolvedType.of(Type.Any);
        }
        else if (type.getType() == Type.ValueVector)
        {
            // Subscript must be either integer/string/object
            if (!(subScriptType.getType() == Type.Int
                    || subScriptType.getType() == Type.Any))
            {
                throw new IllegalArgumentException("Cannot subscript a ValueVector with " + subScriptType.getType());
            }
            return type.getSubType();
        }
        // else if (type.getType() == Type.String)
        // {
        // // Subchar
        // if (!(subScriptType.getType() == Type.Int
        // || subScriptType.getType() == Type.Object))
        // {
        // throw new IllegalArgumentException("Cannot subscript a String with " + subScriptType.getType());
        // }
        //
        // return type;
        // }
        return ResolvedType.of(Type.Any);
    }

    @Override
    public ValueVector eval(TupleVector input, IExecutionContext context)
    {
        ValueVector valueResult = value.eval(input, context);
        ValueVector subScriptResult = subscript.eval(input, context);

        ResolvedType resultType = valueResult.type();
        ResolvedType subScriptType = subScriptResult.type();

        if (resultType.getType() == Type.TupleVector)
        {
            // Subscript must be either integer/string/object
            if (!(subScriptType.getType() == Type.Int
                    || subScriptType.getType() == Type.String
                    || subScriptType.getType() == Type.Any))
            {
                throw new IllegalArgumentException("Cannot subscript a TupleVector with " + subScriptType.getType());
            }

            return getTupleVector(valueResult, subScriptResult);
        }
        else if (resultType.getType() == Type.ValueVector)
        {
            // Subscript must be either integer/string/object
            if (!(subScriptType.getType() == Type.Int
                    || subScriptType.getType() == Type.Any))
            {
                throw new IllegalArgumentException("Cannot subscript a ValueVector with " + subScriptType.getType());
            }

            return getValueVector(valueResult, subScriptResult);
        }
        // else if (resultType.getType() == Type.String)
        // {
        // // Subchar
        // if (!(subScriptType.getType() == Type.Int
        // || subScriptType.getType() == Type.Object))
        // {
        // throw new IllegalArgumentException("Cannot subscript a String with " + subScriptType.getType());
        // }
        //
        //
        //
        // return type;
        // }
        // return ResolvedType.of(Type.Object);
        throw new IllegalArgumentException("Subscript " + resultType + " with " + subScriptType + " is unsupported");
    }

    @Override
    public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    /** Subscript a value vector */
    private ValueVector getValueVector(final ValueVector result, final ValueVector subscript)
    {
        ResolvedType resultType = result.type();
        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return resultType.getSubType();
            }

            @Override
            public int size()
            {
                return result.size();
            }

            @Override
            public boolean isNull(int row)
            {
                // Subscript is null or the nested vector is null
                if (subscript.isNullable()
                        && subscript.isNull(row))
                {
                    return true;
                }
                else if (result.isNullable()
                        && result.isNull(row))
                {
                    return true;
                }

                ValueVector vector = (ValueVector) result.getValue(row);
                int index = subscript.getInt(row);

                // Or we are out of bounds
                return index < 0
                        || index >= vector.size()
                        || vector.isNull(index);
            }

            @Override
            public Object getValue(int row)
            {
                ValueVector vector = (ValueVector) result.getValue(row);
                return vector.getValue(subscript.getInt(row));
            }

            @Override
            public int getInt(int row)
            {
                ValueVector vector = (ValueVector) result.getValue(row);
                return vector.getInt(subscript.getInt(row));
            }

            @Override
            public long getLong(int row)
            {
                ValueVector vector = (ValueVector) result.getValue(row);
                return vector.getLong(subscript.getInt(row));
            }

            @Override
            public float getFloat(int row)
            {
                ValueVector vector = (ValueVector) result.getValue(row);
                return vector.getFloat(subscript.getInt(row));
            }

            @Override
            public double getDouble(int row)
            {
                ValueVector vector = (ValueVector) result.getValue(row);
                return vector.getDouble(subscript.getInt(row));
            }

            @Override
            public boolean getBoolean(int row)
            {
                ValueVector vector = (ValueVector) result.getValue(row);
                return vector.getBoolean(subscript.getInt(row));
            }

            @Override
            public UTF8String getString(int row)
            {
                ValueVector vector = (ValueVector) result.getValue(row);
                return vector.getString(subscript.getInt(row));
            }
        };
    }

    /** Subscript a tuple vector */
    private ValueVector getTupleVector(final ValueVector result, final ValueVector subscript)
    {
        ResolvedType resultType = result.type();
        ResolvedType subScriptType = subscript.type();

        ResolvedType vectorResultType = subScriptType.getType() == Type.Int ? resultType
                : (subScriptType.getType() == Type.String ? ResolvedType.valueVector(ResolvedType.of(Type.Any))
                        : ResolvedType.of(Type.Any));

        return new ValueVector()
        {
            @Override
            public ResolvedType type()
            {
                return vectorResultType;
            }

            @Override
            public int size()
            {
                return result.size();
            }

            @Override
            public boolean isNull(int row)
            {
                if (subscript.isNullable()
                        && subscript.isNull(row))
                {
                    return true;
                }
                else if (result.isNullable()
                        && result.isNull(row))
                {
                    return true;
                }

                // NOTE! Boxing
                Object subScriptValue = subscript.valueAsObject(row);

                TupleVector vector = (TupleVector) result.getValue(row);
                if (subScriptValue instanceof Integer)
                {
                    int index = (int) subScriptValue;
                    return index < 0
                            || index >= vector.getRowCount();
                }
                else if (subScriptValue instanceof UTF8String)
                {
                    String string = ((UTF8String) subScriptValue).toString();
                    return vector.getSchema()
                            .getColumn(string) == null;
                }

                throw new IllegalArgumentException("Cannot subscript a TupleVector with value: " + subScriptValue);
            }

            @Override
            public Object getValue(int row)
            {
                final TupleVector vector = (TupleVector) result.getValue(row);
                Object subScriptValue = subscript.valueAsObject(row);
                if (subScriptValue instanceof Integer)
                {
                    final int index = (int) subScriptValue;
                    return new TupleVector()
                    {
                        @Override
                        public Schema getSchema()
                        {
                            return vector.getSchema();
                        }

                        @Override
                        public int getRowCount()
                        {
                            return 1;
                        }

                        @Override
                        public ValueVector getColumn(int column)
                        {
                            final ValueVector valueVector = vector.getColumn(column);
                            return new ValueVectorAdapter(valueVector)
                            {
                                @Override
                                public int size()
                                {
                                    return 1;
                                }

                                @Override
                                protected int getRow(int row)
                                {
                                    return index;
                                }
                            };
                        }
                    };
                }
                else if (subScriptValue instanceof UTF8String)
                {
                    String string = ((UTF8String) subScriptValue).toString();
                    Column column = vector.getSchema()
                            .getColumn(string);
                    int ordinal = vector.getSchema()
                            .getColumns()
                            .indexOf(column);
                    return vector.getColumn(ordinal);
                }

                throw new IllegalArgumentException("Cannot subscript a TupleVector with value: " + subScriptValue);
            }
        };
    }

    @Override
    public int hashCode()
    {
        // CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + value.hashCode();
        hashCode = hashCode * 37 + subscript.hashCode();
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
        else if (obj instanceof SubscriptExpression)
        {
            SubscriptExpression that = (SubscriptExpression) obj;
            return value.equals(that.value)
                    && subscript.equals(that.subscript);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return value + "[" + subscript + "]";
    }

    @Override
    public String toVerboseString()
    {
        return value.toVerboseString() + "[" + subscript.toVerboseString() + "]";
    }
}
