package se.kuseman.payloadbuilder.core.operator;

import se.kuseman.payloadbuilder.api.operator.Tuple;

/** Definition of a comparator that compares two {@link Tuples} */
interface TupleComparator
{
    /**
     * Compare two tuples.
     *
     * @return Negative if a is smaller than b, Zero if equal, positive if a is larger than b.
     */
    int compare(ExecutionContext context, Tuple a, Tuple b);
}
