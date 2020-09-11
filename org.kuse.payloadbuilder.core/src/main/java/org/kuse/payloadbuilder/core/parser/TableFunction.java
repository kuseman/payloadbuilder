/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.kuse.payloadbuilder.core.catalog.FunctionInfo.validate;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.FunctionInfo;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;
import org.kuse.payloadbuilder.core.catalog.builtin.BuiltinCatalog;
import org.kuse.payloadbuilder.core.operator.TableAlias;

/** Table function */
public class TableFunction extends TableSource
{
    private final String catalogAlias;
    private final String function;
    private final List<Expression> arguments;
    private final int functionId;

    public TableFunction(String catalogAlias, TableAlias tableAlias, String function, List<Expression> arguments, List<Option> options, int functionId, Token token)
    {
        super(tableAlias, options, token);
        this.catalogAlias = catalogAlias;
        this.function = requireNonNull(function, "function");
        this.arguments = requireNonNull(arguments, "arguments");
        this.functionId = functionId;
    }

    @Override
    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    public String getFunction()
    {
        return function;
    }

    public TableFunctionInfo getFunctionInfo(QuerySession session)
    {
        FunctionInfo functionInfo = session.resolveFunctionInfo(catalogAlias, function, functionId);

        if (functionInfo == null)
        {
            throw new ParseException("No function found with name " + function, token);
        }
        else if (!(functionInfo instanceof TableFunctionInfo))
        {
            throw new ParseException("Expected a table valued function but got " + functionInfo, token);
        }

        validate(functionInfo, arguments, token);

        return (TableFunctionInfo) functionInfo;
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public String toString()
    {
        return (catalogAlias != null ? catalogAlias : BuiltinCatalog.NAME) + "#"
            + function
            + "(" + arguments.stream().map(a -> a.toString()).collect(joining(", ")) + ") " + tableAlias.getAlias();
    }
}
