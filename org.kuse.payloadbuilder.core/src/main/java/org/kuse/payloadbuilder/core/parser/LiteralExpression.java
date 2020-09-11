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

import java.util.Objects;

public abstract class LiteralExpression extends Expression
{
    private final Object objectValue;

    protected LiteralExpression(Object value)
    {
        this.objectValue = value;
    }

    public Object getObjectValue()
    {
        return objectValue;
    }

    @Override
    public boolean isConstant()
    {
        return true;
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        return objectValue;
    }

    @Override
    public boolean isNullable()
    {
        return objectValue == null;
    }

    @Override
    public Class<?> getDataType()
    {
        return objectValue != null ? objectValue.getClass() : super.getDataType();
    }

    @Override
    public int hashCode()
    {
        return 17 +
            37 * (objectValue != null ? objectValue.hashCode() : 0);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof LiteralExpression)
        {
            return Objects.equals(objectValue, ((LiteralExpression) obj).objectValue);
        }
        return false;
    }

    /** Create a liteal expression from provided value */
    public static LiteralExpression create(Object value)
    {
        if (value == null)
        {
            return LiteralNullExpression.NULL_LITERAL;
        }
        else if (value instanceof String)
        {
            return new LiteralStringExpression((String) value);
        }
        else if (value instanceof Boolean)
        {
            return (Boolean) value ? LiteralBooleanExpression.TRUE_LITERAL : LiteralBooleanExpression.FALSE_LITERAL;
        }
        else if (value instanceof Double)
        {
            return new LiteralDoubleExpression(((Number) value).doubleValue());
        }
        else if (value instanceof Float)
        {
            return new LiteralFloatExpression(((Number) value).floatValue());
        }
        else if (value instanceof Long)
        {
            return new LiteralLongExpression(((Number) value).longValue());
        }
        else if (value instanceof Integer)
        {
            return new LiteralIntegerExpression(((Number) value).intValue());
        }

        throw new IllegalArgumentException("Cannot create a literal expression from value " + value);
    }

    /** Create a literal numeric expression from provided string */
    public static LiteralExpression createLiteralNumericExpression(String value)
    {
        char forceChar = value.charAt(value.length() - 1);
        if (forceChar == 'l' || forceChar == 'L')
        {
            return new LiteralLongExpression(value.substring(0, value.length() - 1));
        }
        try
        {
            return new LiteralIntegerExpression(value);
        }
        catch (NumberFormatException e)
        {
            try
            {
                return new LiteralLongExpression(value);
            }
            catch (NumberFormatException ee)
            {
                throw new RuntimeException("Cannot create a numeric expression out of " + value, ee);
            }
        }
    }

    /** Create a literal decimal expression from provided string */
    public static LiteralExpression createLiteralDecimalExpression(String value)
    {
        char forceChar = value.charAt(value.length() - 1);
        if (forceChar == 'f' || forceChar == 'F')
        {
            return new LiteralFloatExpression(value.substring(0, value.length() - 1));
        }
        else if (forceChar == 'd' || forceChar == 'D')
        {
            return new LiteralDoubleExpression(value.substring(0, value.length() - 1));
        }
        try
        {
            return new LiteralFloatExpression(value);
        }
        catch (NumberFormatException e)
        {
            try
            {
                return new LiteralDoubleExpression(value);
            }
            catch (NumberFormatException ee)
            {
                throw new RuntimeException("Cannot create a decimal expression out of " + value, ee);
            }
        }
    }
}
