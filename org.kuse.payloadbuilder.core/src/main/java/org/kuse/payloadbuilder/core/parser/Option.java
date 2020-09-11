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

/** Option (WITH) */
public class Option
{
    private final QualifiedName option;
    private final Expression valueExpression;

    Option(QualifiedName option, Expression valueExpression)
    {
        this.option = requireNonNull(option, "option");
        this.valueExpression = requireNonNull(valueExpression, "valueExpression");
    }

    public QualifiedName getOption()
    {
        return option;
    }

    public Expression getValueExpression()
    {
        return valueExpression;
    }

    @Override
    public int hashCode()
    {
        return 37
            + (17 * option.hashCode())
            + (17 * valueExpression.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof Option)
        {
            Option that = (Option) obj;
            return option.equals(that.option)
                && valueExpression.equals(that.valueExpression);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return option + "=" + valueExpression;
    }
}
