package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;

/** Definition of a comparator that compares two rows */
interface RowComparator
{
    /** Compare two rows.
     * @return Negative if a is smaller than b, Zero if equal, positive if a is larger than b. */
    int compare(ExecutionContext context, Row a, Row b);
}
