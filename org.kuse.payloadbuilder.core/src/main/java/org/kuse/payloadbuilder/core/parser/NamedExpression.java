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

import java.util.Objects;

import org.apache.commons.lang3.StringUtils;

/** A named expression used as function arguments etc. */
public class NamedExpression extends Expression
{
    private final String name;
    private final Expression expression;

    public NamedExpression(String name, Expression expression)
    {
        this.name = StringUtils.lowerCase(name);
        this.expression = requireNonNull(expression, "expression");
    }

    public String getName()
    {
        return name;
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public boolean isNullable()
    {
        return expression.isNullable();
    }

    @Override
    public boolean isConstant()
    {
        return expression.isConstant();
    }

    @Override
    public Class<?> getDataType()
    {
        return expression.getDataType();
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        return expression.eval(context);
    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return expression.accept(visitor, context);
    }

    @Override
    public int hashCode()
    {
        return expression.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof NamedExpression)
        {
            NamedExpression that = (NamedExpression) obj;
            return Objects.equals(name, that.name)
                && expression.equals(that.expression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return (name != null ? name + ": " : "") + expression;
    }
}
