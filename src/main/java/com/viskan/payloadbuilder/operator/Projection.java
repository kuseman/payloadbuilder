package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

/** Definition of a projection */
public interface Projection
{
    /** Return value for provided row */
    void writeValue(OutputWriter writer, Row row);
    
    /** Constant projection */
    static class ConstantProjection implements Projection
    {
        private final Object constant;

        ConstantProjection(Object constant)
        {
            this.constant = constant;
        }

        @Override
        public void writeValue(OutputWriter writer, Row row)
        {
            writer.writeValue(constant);
        }
    }
}
