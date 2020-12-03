package org.kuse.payloadbuilder.catalog.es;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Match function that is applicable in query predicates that uses ES match operator on
 * free text fields */
class MatchFunction extends ScalarFunctionInfo
{
    static final String NAME = "match";

    MatchFunction(Catalog catalog)
    {
        super(catalog, NAME);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        throw new IllegalArgumentException("'match' cannot be used in non Elastic query context.");
    }
}

