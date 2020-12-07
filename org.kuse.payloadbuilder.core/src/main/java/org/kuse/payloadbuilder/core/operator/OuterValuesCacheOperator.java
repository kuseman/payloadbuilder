package org.kuse.payloadbuilder.core.operator;

import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Cache operator that sits between a Batch operator and the inner
 * operator to allow for external cache of values */
class OuterValuesCacheOperator extends AOperator
{
    private final Operator operator;

    OuterValuesCacheOperator(
            int nodeId,
            Operator operator,
            Expression cacheKeyExpression)
    {
        super(nodeId);
        this.operator = operator;
    }

    @Override
    public RowIterator open(ExecutionContext context)
    {
        
        
        return null;
    }

    /*
     * batch hash join
     *   scan article
     *   index article_attribute (art_id)
     * 
     * 
     * caching (out = true, key = listOf(a.art_id, @lang_id))
     *   batch hash join
     *     scan article
     *     caching (out = false, listOf(a.art_id, @lang_id)) 
     *       index article_attribute (art_id)
     * 
     * Caching
     *   In-flow
     * 
     *   - Read outer values from context
     *   - Generate cache keys, fetch from cache
     *   - Query down stream index for missing keys
     *     - Re issue outervalues
     *     - Store 
     *   - Iterate new values
     *     - Generate cache keys and put to cache
     *   - Iterate cached values
     * 
     */
}
