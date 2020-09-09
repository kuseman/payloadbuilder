package org.kuse.payloadbuilder.core.catalog;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.LambdaExpression;

/** Interface marking a function as a lambda function */
public interface LambdaFunction
{
    /**
     * Returns lambda bindings pairs. Left expression binds to right lambda expression
     *
     * @param arguments Argument expression to function
     */
    List<Pair<Expression, LambdaExpression>> getLambdaBindings(List<Expression> arguments);

}
