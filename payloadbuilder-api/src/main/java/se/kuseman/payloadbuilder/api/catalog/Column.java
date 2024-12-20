package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Collections.emptyMap;
import static java.util.Collections.unmodifiableMap;
import static java.util.Objects.requireNonNull;

import java.util.Map;

/** A column of a schema */
public class Column
{
    private final String name;
    private final ResolvedType type;
    private final MetaData metaData;

    /** Construct a column with a name and type. */
    public Column(String name, ResolvedType type)
    {
        this(name, type, MetaData.EMPTY);
    }

    /** Construct a column with a name,type and meta data. */
    public Column(String name, ResolvedType type, MetaData metaData)
    {
        this.name = requireNonNull(name, "name");
        this.type = requireNonNull(type, "type");
        this.metaData = requireNonNull(metaData, "metaData");
    }

    public String getName()
    {
        return name;
    }

    public ResolvedType getType()
    {
        return type;
    }

    public MetaData getMetaData()
    {
        return metaData;
    }

    public static Column of(String name, ResolvedType type)
    {
        return new Column(name, type);
    }

    public static Column of(String name, ResolvedType type, MetaData metaData)
    {
        return new Column(name, type, metaData);
    }

    public static Column of(String name, Type type)
    {
        return new Column(name, ResolvedType.of(type));
    }

    public static Column of(String name, Type type, MetaData metaData)
    {
        return new Column(name, ResolvedType.of(type), metaData);
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
                    && type.equals(that.type)
                    && metaData.equals(that.metaData);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return name + " ("
               + type
               + ")"
               + (metaData.properties.isEmpty() ? ""
                       : ", meta: " + metaData.properties.toString());
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

    /**
     * Meta data of a column. Catalogs can provide information like scale/precision/nullable etc. for usage in various places.
     */
    public static class MetaData
    {
        public static final MetaData EMPTY = new MetaData(emptyMap());

        private final Map<String, Object> properties;
        /** Scale of column for decimal types. */
        public static final String SCALE = "scale";
        /** Nullable flag for column. */
        public static final String NULLABLE = "nullable";
        /** Precision of column. Size of string etc. */
        public static final String PRECISION = "precision";

        public MetaData(Map<String, Object> properties)
        {
            this.properties = unmodifiableMap(requireNonNull(properties));
        }

        /** Returns true if column is nullable otherwise false. */
        public boolean isNullable()
        {
            return (boolean) properties.getOrDefault(NULLABLE, true);
        }

        /** Return scale of column or -1 if not set. */
        public int getScale()
        {
            return (int) properties.getOrDefault(SCALE, -1);
        }

        /** Return precision of column or -1 if not set. */
        public int getPrecision()
        {
            return (int) properties.getOrDefault(PRECISION, -1);
        }

        /** Return metadata for provided key. */
        public Object getMetaData(String key)
        {
            return properties.get(requireNonNull(key));
        }

        @Override
        public String toString()
        {
            return properties.toString();
        }

        @Override
        public int hashCode()
        {
            return properties.hashCode();
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
            else if (obj instanceof MetaData that)
            {
                return properties.equals(that.properties);
            }
            return false;
        }
    }
}
