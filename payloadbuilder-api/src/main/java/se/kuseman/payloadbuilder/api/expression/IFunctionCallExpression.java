package se.kuseman.payloadbuilder.api.expression;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo.AggregateMode;

/** Definition of a function call expression */
public interface IFunctionCallExpression extends IExpression
{
    /** Return function info */
    ScalarFunctionInfo getFunctionInfo();

    /** Return aggregate mode */
    AggregateMode getAggregateMode();

    /** Return function arguments */
    List<IExpression> getArguments();

    /** Return catalog alias for this function call */
    String getCatalogAlias();
}
