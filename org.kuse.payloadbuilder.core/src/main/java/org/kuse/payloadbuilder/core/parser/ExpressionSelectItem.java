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

/** Select item that is built from an expression */
public class ExpressionSelectItem extends SelectItem
{
    private final Expression expression;

    public ExpressionSelectItem(Expression expression, String identifier)
    {
        super(getIdentifier(expression, identifier), identifier != null);
        this.expression = requireNonNull(expression, "expression");
    }

    private static String getIdentifier(Expression expression, String identifier)
    {
        if (identifier != null)
        {
            return identifier;
        }

        if (expression instanceof QualifiedReferenceExpression)
        {
            return ((QualifiedReferenceExpression) expression).getQname().getLast();
        }
        else if (expression instanceof DereferenceExpression)
        {
            return ((DereferenceExpression) expression).getRight().getQname().getLast();
        }

        return "No column name";
    }

    public Expression getExpression()
    {
        return expression;
    }

    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public String toString()
    {
        return expression.toString() + super.toString();
    }
}
