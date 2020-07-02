package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.parser.ExecutionContext;

/** Definition of a values extractor.
 * Extracts values from a row into a Object array */
interface ValuesExtractor
{
    /** Extract values */
    void extract(ExecutionContext context, Row row, Object[] values);
}
