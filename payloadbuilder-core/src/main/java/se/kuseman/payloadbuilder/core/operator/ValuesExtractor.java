package se.kuseman.payloadbuilder.core.operator;

import se.kuseman.payloadbuilder.api.operator.Tuple;

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
