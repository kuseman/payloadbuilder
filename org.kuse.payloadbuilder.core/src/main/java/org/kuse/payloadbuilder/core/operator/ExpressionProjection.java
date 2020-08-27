package org.kuse.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;

import org.apache.commons.lang3.time.StopWatch;
import org.kuse.payloadbuilder.core.OutputWriter;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Projection that operates over an {@link Expression} */
class ExpressionProjection implements Projection
{
    private final Expression expression;

    ExpressionProjection(Expression expression)
    {
        this.expression = requireNonNull(expression, "expression");
    }

    StopWatch sw = new StopWatch();
    
    @Override
    public void writeValue(OutputWriter writer, ExecutionContext context)
    {
        sw.start();
        Object value = expression.eval(context);
        context.evalTime.addAndGet(sw.getTime());
        sw.stop();
        sw.reset();
        writer.writeValue(value);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof ExpressionProjection)
        {
            return expression.equals(((ExpressionProjection) obj).expression);
        }
        return false;
    }
}
