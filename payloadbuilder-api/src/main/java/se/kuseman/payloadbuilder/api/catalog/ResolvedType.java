package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import java.util.EnumMap;
import java.util.Objects;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;

/** Holder class for a type resolving for an expression. Contains type and other data needed. For example a {@link Column.Type#TupleVector} needs a {@link Schema} */
public class ResolvedType
{
    private static final EnumMap<Type, ResolvedType> CONSTANTS;

    static
    {
        CONSTANTS = new EnumMap<>(Type.class);
        for (Type type : Type.values())
        {
            if (!(type == Type.TupleVector
                    || type == Type.ValueVector))
            {
                CONSTANTS.put(type, new ResolvedType(type));
            }
        }
    }

    private final Type type;
    /** Type used do specify the contained type if {@link #type} is {@link Type#ValueVector} */
    private final ResolvedType subType;
    private final Schema schema;

    public ResolvedType(Type type)
    {
        this(type, null, null);
    }

    public ResolvedType(Type type, ResolvedType subType, Schema schema)
    {
        if (type == Type.TupleVector
                && schema == null)
        {
            throw new IllegalArgumentException("Must supply schema for a TupleVector type");
        }
        if (type == Type.ValueVector
                && subType == null)
        {
            throw new IllegalArgumentException("Must supply sub type for ValueVector type");
        }

        this.type = requireNonNull(type, "type");
        this.subType = subType;
        this.schema = schema;
    }

    public Type getType()
    {
        return type;
    }

    public ResolvedType getSubType()
    {
        return subType;
    }

    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public int hashCode()
    {
        return type.hashCode();
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
        else if (obj instanceof ResolvedType)
        {
            ResolvedType that = (ResolvedType) obj;
            return type == that.type
                    && Objects.equals(subType, that.subType)
                    && Objects.equals(schema, that.schema);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return type + (schema != null ? ", schema: " + schema
                : "")
                + (subType != null ? "(" + subType + ")"
                        : "");
    }

    /** Return this type as a friendly type string. Resolving all sub types etc. */
    public String toTypeString()
    {
        if (type != Type.ValueVector)
        {
            return type.name();
        }
        return "Array<" + subType.toTypeString() + ">";
    }

    /** Create a resolved type of type TupleVector */
    public static ResolvedType tupleVector(Schema schema)
    {
        return new ResolvedType(Type.TupleVector, null, schema);
    }

    /** Create a resolved type of type ValueVector */
    public static ResolvedType valueVector(ResolvedType type)
    {
        return new ResolvedType(Type.ValueVector, type, null);
    }

    /** Create a resolved type of type ValueVector */
    public static ResolvedType valueVector(Type type)
    {
        return new ResolvedType(Type.ValueVector, ResolvedType.of(type), null);
    }

    /** Get resolved type from provided type. Convenience method when non TupleVector type is used */
    public static ResolvedType of(Type type)
    {
        if (type == Type.TupleVector
                || type == Type.ValueVector)
        {
            throw new IllegalArgumentException("Must supply schema/sub type for a Value and TupleVector types");
        }

        return CONSTANTS.get(type);
    }
}
