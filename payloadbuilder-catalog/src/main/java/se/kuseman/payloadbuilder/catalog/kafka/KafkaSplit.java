package se.kuseman.payloadbuilder.catalog.kafka;

import static java.util.Objects.requireNonNull;

/** Represents a bounded region to read from a single Kafka partition */
record KafkaSplit(String topic, int partition, long startOffset, long endOffset)
{
    KafkaSplit
    {
        requireNonNull(topic, "topic");
    }

    /** Returns true if this split has a finite end offset */
    boolean isBounded()
    {
        return endOffset != Long.MAX_VALUE;
    }

    /** Returns the estimated number of records in this split, or -1 if unbounded */
    long estimatedRecordCount()
    {
        return isBounded() ? endOffset - startOffset
                : -1;
    }

    /** Returns true if this split has been fully consumed at the given offset */
    boolean isComplete(long currentOffset)
    {
        return isBounded()
                && currentOffset >= endOffset;
    }
}
