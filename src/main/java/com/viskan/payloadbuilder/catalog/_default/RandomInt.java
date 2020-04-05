package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;
import com.viskan.payloadbuilder.evaluation.EvaluationContext;
import com.viskan.payloadbuilder.parser.tree.Expression;

import java.util.List;
import java.util.Random;

/** Function random int, returns a random int from provided seed. */
class RandomInt extends ScalarFunctionInfo
{
    private final Random random;

    RandomInt(Catalog catalog)
    {
        super(catalog, "randomInt", Type.SCALAR);
        this.random = new Random();
    }
    
    @Override
    public Object eval(EvaluationContext context, List<Expression> arguments, Row row)
    {
        int bound = ((Number) arguments.get(0).eval(context, row)).intValue();
        return random.nextInt(bound);
    }
}
