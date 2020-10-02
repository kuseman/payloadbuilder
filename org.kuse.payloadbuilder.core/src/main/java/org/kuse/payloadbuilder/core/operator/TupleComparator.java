package org.kuse.payloadbuilder.core.operator;

import org.kuse.payloadbuilder.core.parser.ExecutionContext;

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
