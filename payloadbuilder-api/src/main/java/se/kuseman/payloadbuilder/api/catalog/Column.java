package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

/** A column of a schema */
public class Column
{
    private final String name;
    private final ResolvedType type;

    /** Construct a column with a type and column reference */
    public Column(String name, ResolvedType type)
    {
        this.name = requireNonNull(name, "name");
        this.type = requireNonNull(type, "type");
    }

    public String getName()
    {
        return name;
    }

    public ResolvedType getType()
    {
        return type;
    }

    public static Column of(String name, ResolvedType type)
    {
        return new Column(name, type);
    }

    public static Column of(String name, Type type)
    {
        return new Column(name, ResolvedType.of(type));
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
        else if (getClass() == obj.getClass())
        {
            Column that = (Column) obj;
            return name.equals(that.name)
                    && type.equals(that.type);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return name + " (" + type + ")";
    }

    /** Data type of column */
    public enum Type
    {
        /** Unkown type. Can be arbitrary value that is used reflectively runtime */
        Any(0, false, false, false),
        String(30, false, false, false),
        Boolean(40, false, false, true),
        Int(50, true, false, true),
        Long(60, true, false, true),
        Decimal(65, true, false, false),
        Float(70, true, false, true),
        Double(80, true, false, true),
        DateTime(90, false, false, false),
        DateTimeOffset(100, false, false, false),

        /** A object with key value pairs */
        Object(1000, false, true, false),
        /** Array of values */
        Array(2000, false, true, false),
        /** Nested table. Ie. result from a populated join */
        Table(3000, false, true, false);

        private final int precedence;
        private final boolean number;
        private final boolean complex;
        private final boolean primitive;

        Type(int precedence, boolean number, boolean complex, boolean primitive)
        {
            this.precedence = precedence;
            this.number = number;
            this.complex = complex;
            this.primitive = primitive;
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

        public boolean isPrimitive()
        {
            return primitive;
        }
    }
}
