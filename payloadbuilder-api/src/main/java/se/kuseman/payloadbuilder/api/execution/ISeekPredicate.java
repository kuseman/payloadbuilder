package se.kuseman.payloadbuilder.api.execution;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.Index;

/** Index seek predicate used when creating index seek scan operators from catalogs */
public interface ISeekPredicate
{
    /** Return the index for this predicate */
    Index getIndex();

    /** Return the used columns for this predicate from {@link #getIndex()} */
    List<String> getIndexColumns();

    /**
     * Returns seek keys for this predicate. Size of keys equals the size of the {@link #getIndexColumns()} used.
     */
    List<ISeekKey> getSeekKeys(IExecutionContext context);

    /** Definition of a seek key. Part of a seek predicate that corresponds to one of the columns used */
    interface ISeekKey
    {
        /** Return the value vector that represents this keys values */
        ValueVector getValue();

        /** Return the seek type for this key */
        SeekType getType();
    }

    /** Definition of a seek keys type */
    public enum SeekType
    {
        /** Equal. Used to seek rows equal to provided values */
        EQ("=");

        private final String sign;

        private SeekType(String sign)
        {
            this.sign = sign;
        }

        public String getSign()
        {
            return sign;
        }
    }
}
