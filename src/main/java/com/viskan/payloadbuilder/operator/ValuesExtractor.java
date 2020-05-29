package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;

/** Definition of a values extractor.
 * Extracts values from a row into a Object array */
public interface ValuesExtractor
{
    /** Extract values */
    void extract(EvaluationContext context, Row row, Object[] values);
}
