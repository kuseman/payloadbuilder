package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.parser.Expression;
import com.viskan.payloadbuilder.parser.LambdaExpression;

import java.util.List;

import org.apache.commons.lang3.tuple.Pair;

/** Interface marking a function as a lambda function */
public interface LambdaFunction
{
    /** Returns lambda bindings pairs.
     * Left expression binds to right lambda expression 
     * @param arguments Argument expression to function */
    List<Pair<Expression, LambdaExpression>> getLambdaBindings(List<Expression> arguments);
    
}
