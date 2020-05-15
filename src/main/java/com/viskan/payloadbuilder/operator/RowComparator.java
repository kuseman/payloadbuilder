package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

/** Definition of a comparator that compares two rows */
public interface RowComparator
{
    /** Compare two rows.
     * @return Negative if a is smaller than b, Zero if equal, positive if a is larger than b. */
    int compare(EvaluationContext context, Row a, Row b);
}
