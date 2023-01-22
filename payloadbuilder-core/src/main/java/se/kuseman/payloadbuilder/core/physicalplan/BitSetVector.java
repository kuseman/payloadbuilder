package se.kuseman.payloadbuilder.core.physicalplan;

import java.util.BitSet;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** {@link ValueVector} implementation based on {@link BitSet} */
public class BitSetVector implements ValueVector
{
    private final int size;
    private final BitSet bitSet;
    private final BitSet nullBs;

    public BitSetVector(int size, BitSet bitSet)
    {
        this(size, bitSet, null);
    }

    public BitSetVector(int size, BitSet bitSet, BitSet nullBs)
    {
        this.size = size;
        this.bitSet = bitSet;
        // If we don't have any nulls set this vector to non nullable
        this.nullBs = nullBs != null
                && nullBs.cardinality() > 0 ? nullBs
                        : null;
    }

    @Override
    public ResolvedType type()
    {
        return ResolvedType.of(Type.Boolean);
    }

    @Override
    public boolean isNull(int row)
    {
        if (nullBs == null)
        {
            return false;
        }
        return nullBs.get(row);
    }

    @Override
    public int size()
    {
        return size;
    }

    @Override
    public Object getAny(int row)
    {
        if (isNull(row))
        {
            return null;
        }
        return bitSet.get(row);
    }

    @Override
    public boolean getBoolean(int row)
    {
        if (isNull(row))
        {
            throw new NullPointerException("Row value is null");
        }
        return bitSet.get(row);
    }

    @Override
    public boolean getPredicateBoolean(int row)
    {
        if (nullBs == null)
        {
            return bitSet.get(row);
        }

        return !nullBs.get(row)
                && bitSet.get(row);
    }

    /** Convert provided value vector to a bitset variant */
    public static BitSetVector convert(ValueVector vector)
    {
        return convert(vector, false);
    }

    /** Convert provided value vector to a bitset variant */
    public static BitSetVector convert(ValueVector vector, boolean invert)
    {
        if (vector.type()
                .getType() != Type.Boolean)
        {
            throw new IllegalArgumentException("Converting a Vector to bitset requires type of boolean");
        }

        int size = vector.size();
        BitSet bitSet = new BitSet(size);
        BitSet nullSet = null;

        for (int i = 0; i < size; i++)
        {
            if (vector.isNull(i))
            {
                if (nullSet == null)
                {
                    nullSet = new BitSet(size);
                }
                nullSet.set(i, true);
                continue;
            }

            boolean value = vector.getBoolean(i);
            bitSet.set(i, invert ? !value
                    : value);
        }
        return new BitSetVector(size, bitSet, nullSet);
    }

    /**
     * And this vector with provided.
     *
     * @param other The vector to perform and against
     */
    public BitSetVector and(ValueVector other)
    {
        if (size != other.size())
        {
            throw new IllegalArgumentException("Both bit set vectors must equal in size to perform AND");
        }

        BitSetVector that;
        if (!(other instanceof BitSetVector))
        {
            that = convert(other);
        }
        else
        {
            that = (BitSetVector) other;
        }

        BitSet result = (BitSet) bitSet.clone();
        // both non nullable => perform simple AND
        if (nullBs == null
                && that.nullBs == null)
        {
            result.and(that.bitSet);
            return new BitSetVector(size, result, null);
        }

        BitSet nullResult = new BitSet(size);
        // Calculate NVL
        for (int i = 0; i < size; i++)
        {
            boolean thisNull = nullBs != null
                    && nullBs.get(i);
            boolean thatNull = that.nullBs != null
                    && that.nullBs.get(i);

            if (thisNull
                    && thatNull)
            {
                // null AND null => null
                nullResult.set(i, true);
            }
            else if (thisNull)
            {
                // null AND true => null
                // null AND false => false
                nullResult.set(i, that.bitSet.get(i));
            }
            else if (thatNull)
            {
                // true AND null => null
                // false AND null => false
                nullResult.set(i, bitSet.get(i));
            }
            else
            {
                result.set(i, bitSet.get(i)
                        && that.bitSet.get(i));
            }
        }

        return new BitSetVector(size, result, nullResult);
    }

    /**
     * Or this vector with provided.
     *
     * @param other The vector to perform or against
     */
    public BitSetVector or(ValueVector other)
    {
        if (size != other.size())
        {
            throw new IllegalArgumentException("Both bit set vectors must equal in size to perform OR");
        }

        BitSet nullResult = null;

        BitSetVector that;
        if (!(other instanceof BitSetVector))
        {
            that = convert(other);
        }
        else
        {
            that = (BitSetVector) other;
        }

        BitSet result = (BitSet) bitSet.clone();
        BitSet thatBitSet = that.bitSet;
        BitSet thatNullBitSet = that.nullBs;

        // Combine both null bit sets
        if (nullBs != null)
        {
            nullResult = (BitSet) nullBs.clone();
        }
        if (thatNullBitSet != null)
        {
            if (nullResult != null)
            {
                nullResult.or(thatNullBitSet);
            }
            else
            {
                nullResult = thatNullBitSet;
            }
        }

        result.or(thatBitSet);

        // Clear all null bits for all true bits
        // since true or null is true
        if (nullResult != null)
        {
            for (int i = 0; i < size; i++)
            {
                if (result.get(i))
                {
                    nullResult.set(i, false);
                }
            }
        }

        return new BitSetVector(size, result, nullResult);
    }

    /**
     * Invert with vector
     */
    public BitSetVector not()
    {
        BitSet bs = (BitSet) bitSet.clone();

        for (int i = 0; i < size; i++)
        {
            // Keep nulls since not null is null (note this is not a null predicate 'IS (NOT)? NULL`
            if (nullBs != null
                    && nullBs.get(i))
            {
                continue;
            }
            bs.set(i, !bs.get(i));
        }
        return new BitSetVector(size, bs, nullBs);
    }

    /** Create an inverted bit set vector from provided vector */
    public static ValueVector not(ValueVector vector)
    {
        if (vector instanceof BitSetVector)
        {
            return ((BitSetVector) vector).not();
        }
        return convert(vector, true);
    }
}