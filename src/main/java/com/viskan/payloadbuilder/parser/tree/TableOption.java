package com.viskan.payloadbuilder.parser.tree;

/** With option declared on from clause */
public abstract class TableOption extends WithOption
{
    /** Batch size option */
    public static class BatchSizeOption extends TableOption
    {
        private final int batchSize;
        public BatchSizeOption(int batchSize)
        {
            this.batchSize = batchSize;
        }
        
        public int getBatchSize()
        {
            return batchSize;
        }
        
        @Override
        public int hashCode()
        {
            return batchSize;
        }
        
        @Override
        public boolean equals(Object obj)
        {
            if (obj instanceof BatchSizeOption)
            {
                BatchSizeOption that = (BatchSizeOption) obj;
                return batchSize == that.batchSize;
            }
            return false;
        }
        
        @Override
        public String toString()
        {
            return "BATCH_SIZE = " + batchSize;
        }
    }
}
