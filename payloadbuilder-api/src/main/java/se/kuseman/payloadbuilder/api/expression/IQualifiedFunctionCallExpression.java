package se.kuseman.payloadbuilder.api.expression;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;

/** Definition of a function call expression */
public interface IQualifiedFunctionCallExpression extends IExpression
{
    /** Return function info */
    ScalarFunctionInfo getFunctionInfo();

    /** Return IN arguments */
    List<? extends IExpression> getArguments();
}
