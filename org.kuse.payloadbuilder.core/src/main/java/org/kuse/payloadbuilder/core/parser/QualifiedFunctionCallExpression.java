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
import java.util.Objects;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.FunctionInfo;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;

/** Scalar function call */
public class QualifiedFunctionCallExpression extends Expression
{
    private final String catalog;
    private final String function;
    private final List<Expression> arguments;

    /**
     * Unique id for this function call in query. Used to store function lookup during evaluation to avoid multiple lookups
     */
    private final int functionId;

    private final Token token;

    QualifiedFunctionCallExpression(
            String catalog,
            String function,
            List<Expression> arguments,
            int functionId)
    {
        this(catalog, function, arguments, functionId, null);
    }

    QualifiedFunctionCallExpression(
            String catalog,
            String function,
            List<Expression> arguments,
            int functionId,
            Token token)
    {
        this.catalog = catalog;
        this.function = requireNonNull(function, "function");
        this.arguments = requireNonNull(arguments, "arguments");
        this.functionId = functionId;
        this.token = token;
    }

    public String getCatalog()
    {
        return catalog;
    }

    public String getFunction()
    {
        return function;
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }

    /** Resolves function this expression based on provided session */
    public ScalarFunctionInfo getFunctionInfo(QuerySession session)
    {
        FunctionInfo functionInfo = session.resolveFunctionInfo(catalog, function, functionId);

        if (functionInfo == null)
        {
            throw new ParseException("No function found with name " + function, token);
        }
        else if (!(functionInfo instanceof ScalarFunctionInfo))
        {
            throw new ParseException("Expected a scalar function but got " + functionInfo, token);
        }

        validate(functionInfo, arguments, token);

        return (ScalarFunctionInfo) functionInfo;

        /*
         * QuerySession (Static data through whole execution, exception of properties)
         *   Catalogs
         *   Params
         *
         * OperatorContext (Context used during selects)
         *   NodeDat
         *
         *
         * EvaluationContext (Context used during evaluation)
         *   LambdaValues
         *
         *
         * ExecutionContext
         *   QuerySession
         *   EvaluationContext (cleared before each evalution)
         *
         */

        //        throw new IllegalArgumentException("No function named: " + qname.getLast() + " found in " + qname.getCatalog());
        //        return null;
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        ScalarFunctionInfo functionInfo = getFunctionInfo(context.getSession());
        return functionInfo.eval(context, arguments);
    }

    //    @Override
    //    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    //    {
    //        return null;//functionInfo.generateCode(context, parentCode, arguments);
    //    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public Class<?> getDataType()
    {
        return Object.class;// functionInfo.getDataType();
    }

    @Override
    public boolean isNullable()
    {
        return true;//functionInfo.isNullable();
    }

    @Override
    public int hashCode()
    {
        return 17 +
            (37 * function.hashCode()) +
            (37 * arguments.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof QualifiedFunctionCallExpression)
        {
            QualifiedFunctionCallExpression that = (QualifiedFunctionCallExpression) obj;
            return Objects.equals(catalog, that.catalog)
                && function.equals(that.function)
                && arguments.equals(that.arguments);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return catalog != null ? (catalog + "#")
            : ""
                + function
                + "(" + arguments.stream().map(a -> a.toString()).collect(joining(", ")) + ")";
    }
}
