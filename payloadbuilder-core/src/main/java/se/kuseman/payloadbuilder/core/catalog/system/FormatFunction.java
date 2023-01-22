package se.kuseman.payloadbuilder.core.catalog.system;

import java.time.ZonedDateTime;
import java.time.format.DateTimeFormatter;
import java.util.List;
import java.util.Locale;

import org.apache.commons.lang3.LocaleUtils;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.catalog.TupleVector;
import se.kuseman.payloadbuilder.api.catalog.UTF8String;
import se.kuseman.payloadbuilder.api.catalog.ValueVector;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Format function that can format numeric and date time values */
class FormatFunction extends ScalarFunctionInfo
{
    FormatFunction(Catalog catalog)
    {
        super(catalog, "format", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<? extends IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<? extends IExpression> arguments)
    {
        if (!(arguments.size() == 2
                || arguments.size() == 3))
        {
            throw new IllegalArgumentException("Function " + getName() + " expects 2 or 3 arguments");
        }

        final ValueVector value = arguments.get(0)
                .eval(input, context);
        final ValueVector format = arguments.get(1)
                .eval(input, context);

        IExpression localeExpression = arguments.size() == 3 ? arguments.get(2)
                : null;

        final ValueVector locale = localeExpression != null ? localeExpression.eval(input, context)
                : null;

        final String constantFormat = arguments.get(1)
                .isConstant()
                        ? format.getString(0)
                                .toString()
                        : null;
        final String constantLocale = localeExpression != null
                && localeExpression.isConstant()
                        ? locale.getString(0)
                                .toString()
                        : null;

        return new ValueVector()
        {
            DateTimeFormatter constantDateTimeFormatter;

            @Override
            public ResolvedType type()
            {
                return ResolvedType.of(Type.String);
            }

            @Override
            public int size()
            {
                return input.getRowCount();
            }

            @Override
            public boolean isNullable()
            {
                return value.isNullable()
                        || format.isNullable()
                        || (locale != null
                                && locale.isNullable());
            }

            @Override
            public boolean isNull(int row)
            {
                return value.isNull(row)
                        || format.isNull(row)
                        || (locale != null
                                && locale.isNull(row));
            }

            @Override
            public UTF8String getString(int row)
            {
                String formatStr = constantFormat;
                if (formatStr == null)
                {
                    formatStr = format.getString(row)
                            .toString();
                }

                Locale currentLocale = null;
                if (locale != null)
                {
                    String localeStr = constantLocale;
                    if (localeStr == null)
                    {
                        localeStr = locale.getString(row)
                                .toString();
                    }

                    currentLocale = LocaleUtils.toLocale(localeStr);
                }

                Object val;

                // For date times we use date time formatter in stead of string format
                if (value.type()
                        .getType() == Type.DateTime)
                {
                    val = value.getDateTime(row)
                            .toZonedDateTime();

                    DateTimeFormatter dateTimeFormatter;
                    if (constantFormat != null)
                    {
                        if (constantDateTimeFormatter == null)
                        {
                            if (currentLocale != null)
                            {
                                constantDateTimeFormatter = DateTimeFormatter.ofPattern(formatStr, currentLocale);
                            }
                            else
                            {
                                constantDateTimeFormatter = DateTimeFormatter.ofPattern(formatStr);
                            }
                        }
                        dateTimeFormatter = constantDateTimeFormatter;
                    }
                    else
                    {
                        dateTimeFormatter = DateTimeFormatter.ofPattern(formatStr);
                    }

                    return UTF8String.from(dateTimeFormatter.format((ZonedDateTime) val));
                }
                else
                {
                    // Boxing for now
                    val = value.valueAsObject(row);
                }

                if (currentLocale != null)
                {
                    return UTF8String.from(String.format(currentLocale, formatStr, val));
                }

                return UTF8String.from(String.format(formatStr, val));
            }

            @Override
            public Object getValue(int row)
            {
                throw new IllegalArgumentException("getValue should not be called on typed vectors");
            }
        };
    }
}
