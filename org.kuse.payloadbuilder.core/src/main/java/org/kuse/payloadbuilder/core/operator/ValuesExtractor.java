package org.kuse.payloadbuilder.core.operator;

/**
 * Definition of a values extractor. Extracts values from a {@link Tuple} into an Object array
 */
interface ValuesExtractor
{
    /** Extract values */
    void extract(ExecutionContext context, Tuple tuple, Object[] values);

    /** Number of values that this extractor extracts */
    int size();
}
