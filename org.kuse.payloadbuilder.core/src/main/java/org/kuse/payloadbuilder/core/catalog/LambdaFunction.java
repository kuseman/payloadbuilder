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
     * <pre>
     * This is used to be able to correctly analyze expression return types to connect
     * which fields belongs to which aliases in a query.
     * 
     * Ie. 
     * 
     * <i>map(list, x -> x.id)</i>
     * Here argument <b>list</b> binds to the lambda expression <b>x -> x.id</b>.
     * </pre>
     * @param arguments Argument expression to function
     */
    List<Pair<Expression, LambdaExpression>> getLambdaBindings(List<Expression> arguments);

}
