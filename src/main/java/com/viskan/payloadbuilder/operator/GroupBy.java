package com.viskan.payloadbuilder.operator;

import com.viskan.payloadbuilder.GroupedRow;
import com.viskan.payloadbuilder.Row;

import static java.util.Objects.requireNonNull;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.function.Function;

import org.apache.commons.collections.Transformer;
import org.apache.commons.collections.iterators.TransformIterator;
import org.apache.commons.lang3.StringUtils;

import gnu.trove.map.hash.THashMap;

/** Operator that groups by a bucket function */
class GroupBy implements Operator
{
    private final Operator operator;
    private final Function<Row, Object> bucketFunction;

    GroupBy(Operator operator, Function<Row, Object> bucketFunction)
    {
        this.operator = requireNonNull(operator);
        this.bucketFunction = requireNonNull(bucketFunction);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Iterator<Row> open(OperatorContext context)
    {
        Map<Object, List<Row>> table = new THashMap<>();
        Iterator<Row> it = operator.open(context);
        while (it.hasNext())
        {
            Row row = it.next();
            Object bucket = bucketFunction.apply(row);
            table.computeIfAbsent(bucket, key -> new ArrayList<>()).add(row);
        }

        return new TransformIterator(table.values().iterator(), new Transformer()
        {
            int position = 0;

            @Override
            public Object transform(Object input)
            {
                return new GroupedRow((List<Row>) input, position++);
            }
        });
    }

    @Override
    public String toString(int indent)
    {
        String indentString = StringUtils.repeat("  ", indent);
        return "GROUP BY" + System.lineSeparator()
            +
            indentString + operator.toString(indent + 1);
    }
}
