package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.function.Function;

import org.apache.commons.collections.iterators.TransformIterator;

/**
 * Lookup's values for a key used later in pipeline.
 * 
 * <pre>
 *  from source s
 *  inner join articleAttribute aa
 *    on aa.art_id = s.art_id
 *
 *  Here we have a lookup-index on articleArticle on
 *  column art_id
 *  Then a KeyLookup is placed before the scan on source
 *  to extract art_id's that can later be used by articleAttribute
 *  scan
 *  Used in conjunction with HashMatch with forced hashing on lookup side.
 *  That is source in example above.
 *  Can only be used when condition is equijoin
 * </pre>
 **/
class KeyLookup implements Operator
{
    private final Operator operator;
    private final Function<Row, Object> valueExtractor;
    private final String key;

    KeyLookup(Operator operator, String key, Function<Row, Object> valueExtractor)
    {
        this.operator = requireNonNull(operator, "operator");
        this.key = requireNonNull(key, "key");
        this.valueExtractor = requireNonNull(valueExtractor, "valueExtractor");
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        final List<Object> indexLookupValues = new ArrayList<>();
        context.addIndexLookupValues(key, indexLookupValues);
        return new TransformIterator(operator.open(context), input ->
        {
            Row row = (Row) input;
            indexLookupValues.add(valueExtractor.apply(row));
            return row;
        });
    }
}
