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
package org.kuse.payloadbuilder.core.catalog.builtin;

import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.time.temporal.TemporalAccessor;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

/** Cast and convert function */
class CastFunction extends ScalarFunctionInfo
{
    private final static DateTimeFormatter ISO_DATE_OPTIONAL_TIME_FORMATTER = DateTimeFormatter.ofPattern("yyyy-MM-dd['T'HH:mm[:ss][.SSS][X]]");

    CastFunction(Catalog catalog, String name)
    {
        super(catalog, name);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object source = arguments.get(0).eval(context);
        if (source == null)
        {
            return null;
        }
        String dataTypeString;
        Expression dataTypeExpression = arguments.get(1);
        if (dataTypeExpression instanceof QualifiedReferenceExpression)
        {
            dataTypeString = ((QualifiedReferenceExpression) dataTypeExpression).getQname().toString();
        }
        else
        {
            Object obj = dataTypeExpression.eval(context);
            if (obj == null)
            {
                return null;
            }
            dataTypeString = String.valueOf(obj);
        }

        DataType dataType = DataType.valueOf(dataTypeString.toUpperCase());

        if (dataType.type.isAssignableFrom(source.getClass()))
        {
            return source;
        }

        int style = 0;
        if (arguments.size() >= 3)
        {
            Object styleObject = arguments.get(2).eval(context);
            if (!(styleObject instanceof Integer))
            {
                throw new IllegalArgumentException("Style argument of " + getName() + " must be of integer type.");
            }
            style = ((Integer) styleObject).intValue();
        }

        return dataType.convert(source, style);
    }

    enum DataType
    {
        BOOLEAN(Boolean.class)
        {
            @Override
            Object convert(Object source, int style)
            {
                if (source instanceof Number)
                {
                    return ((Number) source).intValue() != 0;
                }
                else if (source instanceof String)
                {
                    return Boolean.parseBoolean((String) source);
                }
                throw new IllegalArgumentException("Cannot cast " + source + " to Boolean.");
            }
        },
        DATETIME(LocalDateTime.class)
        {
            @Override
            Object convert(Object source, int style)
            {
                // Time in millis
                if (source instanceof Long)
                {
                    return LocalDateTime.ofInstant(Instant.ofEpochMilli(((Long) source).longValue()), ZoneId.systemDefault());
                }
                else if (source instanceof String)
                {
                    String string = ((String) source).replace(' ', 'T');
                    TemporalAccessor parsed = ISO_DATE_OPTIONAL_TIME_FORMATTER.parseBest(string, ZonedDateTime::from, LocalDateTime::from, LocalDate::from);
                    if (parsed instanceof ZonedDateTime)
                    {
                        ZonedDateTime localZoned = ((ZonedDateTime) parsed).withZoneSameInstant(ZoneId.systemDefault());
                        return localZoned.toLocalDateTime();
                    }
                    if (parsed instanceof LocalDateTime)
                    {
                        return parsed;
                    }
                    else if (parsed instanceof LocalDate)
                    {
                        return ((LocalDate) parsed).atStartOfDay();
                    }
                }
                else if (source instanceof ZonedDateTime)
                {
                    return ((ZonedDateTime) source).withZoneSameInstant(ZoneId.systemDefault()).toLocalDateTime();
                }
                throw new IllegalArgumentException("Cannot cast " + source + " to DateTime.");
            }
        },
        DATETIMEOFFSET(ZonedDateTime.class)
        {
            @Override
            Object convert(Object source, int style)
            {
                // Time in millis
                if (source instanceof Long)
                {
                    return ZonedDateTime.ofInstant(Instant.ofEpochMilli(((Long) source).longValue()), ZoneOffset.UTC);
                }
                else if (source instanceof String)
                {
                    return ZonedDateTime.parse((String) source);
                }
                else if (source instanceof LocalDateTime)
                {
                    return ((LocalDateTime) source).atZone(ZoneId.systemDefault());
                }

                throw new IllegalArgumentException("Cannot cast " + source + " to DateTimeOffset.");
            }
        },
        DOUBLE(Double.class)
        {
            @Override
            Object convert(Object source, int style)
            {
                if (source instanceof Number)
                {
                    return ((Number) source).doubleValue();
                }
                else if (source instanceof String)
                {
                    return Double.parseDouble((String) source);
                }
                throw new IllegalArgumentException("Cannot cast " + source + " to Double.");
            }
        },
        FLOAT(Float.class)
        {
            @Override
            Object convert(Object source, int style)
            {
                if (source instanceof Number)
                {
                    return ((Number) source).floatValue();
                }
                else if (source instanceof String)
                {
                    return Float.parseFloat((String) source);
                }
                throw new IllegalArgumentException("Cannot cast " + source + " to Float.");
            }
        },
        INTEGER(Integer.class)
        {
            @Override
            Object convert(Object source, int style)
            {
                if (source instanceof Number)
                {
                    return ((Number) source).intValue();
                }
                else if (source instanceof String)
                {
                    return Integer.parseInt((String) source);
                }
                throw new IllegalArgumentException("Cannot cast " + source + " to Integer.");
            }
        },
        LONG(Long.class)
        {
            @Override
            Object convert(Object source, int style)
            {
                if (source instanceof Number)
                {
                    return ((Number) source).longValue();
                }
                else if (source instanceof String)
                {
                    return Long.parseLong((String) source);
                }
                throw new IllegalArgumentException("Cannot cast " + source + " to Long.");
            }
        },
        STRING(String.class)
        {
            @Override
            Object convert(Object source, int style)
            {
                return String.valueOf(source);
            }
        };

        final Class<?> type;

        DataType(Class<?> type)
        {
            this.type = type;
        }

        /** Convert provided source */
        abstract Object convert(Object source, int style);
    }
}
