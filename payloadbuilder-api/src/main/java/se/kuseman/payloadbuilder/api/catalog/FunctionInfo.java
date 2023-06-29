package se.kuseman.payloadbuilder.api.catalog;

import static java.util.Objects.requireNonNull;

import se.kuseman.payloadbuilder.api.execution.TupleVector;

/** Base class for functions */
public abstract class FunctionInfo
{
    private final String name;
    private final FunctionType type;

    public FunctionInfo(String name, FunctionType type)
    {
        this.name = requireNonNull(name, "name");
        this.type = requireNonNull(type, "type");
    }

    public String getName()
    {
        return name;
    }

    /** Description of function. Used in show statement for a description of the function. */
    public String getDescription()
    {
        return "";
    }

    public FunctionType getFunctionType()
    {
        return type;
    }

    /** Returns true if all arguments should be named for this function else false. */
    public boolean requiresNamedArguments()
    {
        return false;
    }

    /** Return this functions arity. */
    public Arity arity()
    {
        return Arity.NO_LIMIT;
    }

    @Override
    public String toString()
    {
        return name;
    }

    @Override
    public int hashCode()
    {
        return name.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof FunctionInfo)
        {
            FunctionInfo that = (FunctionInfo) obj;
            return name.equals(that.name)
                    && type.equals(that.type);
        }
        return false;
    }

    /** Function type */
    public enum FunctionType
    {
        /** A scalar function used in expressions returning a scalar value. */
        SCALAR,
        /** A scalar function used in aggregations returning a scalar value from groups of values */
        AGGREGATE,
        /** A scalar function that act as both scalar and aggregate depending on context. */
        SCALAR_AGGREGATE,
        /** A table valued function used in function scans return a stream of {@link TupleVector} */
        TABLE,
        /** An operator function used in operators that supports transforming input stream to a scalar value. Ie. FOR clause */
        OPERATOR;

        /** Return true if this type is of aggregate type */
        public boolean isAggregate()
        {
            return this == AGGREGATE
                    || this == SCALAR_AGGREGATE;
        }
    }

    /** Arity definition for functions */
    public static class Arity
    {
        /** Arity where the arguments have no limits */
        public static final Arity NO_LIMIT = new Arity(-1, -1);
        /** Arity for functions with no arguments */
        public static final Arity ZERO = new Arity(0, 0);
        /** Arity for functions with one argument */
        public static final Arity ONE = new Arity(1, 1);
        /** Arity for functions with at least one argument */
        public static final Arity AT_LEAST_ONE = new Arity(1, -1);
        /** Arity for functions with two arguments */
        public static final Arity TWO = new Arity(2, 2);
        /** Arity for functions with at least two arguments */
        public static final Arity AT_LEAST_TWO = new Arity(2, -1);

        /** Mininum arguments */
        private final int min;
        /** Mininum arguments */
        private final int max;

        public Arity(int min, int max)
        {
            this.min = min;
            this.max = max;

            if (min >= 0
                    && max >= 0
                    && max < min)
            {
                throw new IllegalArgumentException("Invalid arity definition. max must be greater of equal to min");
            }
        }

        public int getMin()
        {
            return min;
        }

        public int getMax()
        {
            return max;
        }

        /** Returns true if this arity satisfies provided argument count */
        public boolean satisfies(int argumentCount)
        {
            if (min < 0)
            {
                return true;
            }
            return argumentCount >= min
                    && (max < 0
                            || argumentCount <= max);
        }

        @Override
        public int hashCode()
        {
            return min;
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
            else if (obj instanceof Arity)
            {
                Arity that = (Arity) obj;
                return min == that.min
                        && max == that.max;
            }

            return false;
        }
    }
}
