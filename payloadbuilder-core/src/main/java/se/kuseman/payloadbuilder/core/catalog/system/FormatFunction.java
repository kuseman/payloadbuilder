package se.kuseman.payloadbuilder.core.catalog.system;

import java.text.DecimalFormat;
import java.text.DecimalFormatSymbols;
import java.text.NumberFormat;
import java.time.format.DateTimeFormatter;
import java.time.temporal.Temporal;
import java.util.List;

import org.apache.commons.lang3.LocaleUtils;

import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.EpochDateTime;
import se.kuseman.payloadbuilder.api.execution.EpochDateTimeOffset;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.execution.vector.IObjectVectorBuilder;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Format function that can format numeric and date time values */
class FormatFunction extends ScalarFunctionInfo
{
    FormatFunction()
    {
        super("format", FunctionType.SCALAR);
    }

    @Override
    public ResolvedType getType(List<IExpression> arguments)
    {
        return ResolvedType.of(Type.String);
    }

    @Override
    public Arity arity()
    {
        return new Arity(2, 3);
    }

    @Override
    public ValueVector evalScalar(IExecutionContext context, TupleVector input, String catalogAlias, List<IExpression> arguments)
    {
        ValueVector value = arguments.get(0)
                .eval(input, context);
        ValueVector format = arguments.get(1)
                .eval(input, context);

        boolean hasLocale = arguments.size() == 3;
        IExpression localeExpression = hasLocale ? arguments.get(2)
                : null;

        ValueVector locale = hasLocale ? localeExpression.eval(input, context)
                : null;

        String constantFormat = arguments.get(1)
                .isConstant() ? format.valueAsString(0)
                        : null;
        String constantLocale = hasLocale
                && localeExpression.isConstant() ? locale.valueAsString(0)
                        : null;

        int rowCount = input.getRowCount();
        IObjectVectorBuilder builder = context.getVectorBuilderFactory()
                .getObjectVectorBuilder(ResolvedType.of(Type.String), rowCount);

        Type valueType = value.type()
                .getType();

        NumberFormat constantNumberFormat = null;
        DateTimeFormatter constantDateTimeFormatter = null;

        for (int i = 0; i < rowCount; i++)
        {
            if (value.isNull(i)
                    || format.isNull(i)
                    || (locale != null
                            && locale.isNull(i)))
            {
                builder.putNull();
                continue;
            }

            Object anyValue = null;

            if (valueType == Type.Any)
            {
                anyValue = value.valueAsObject(i);
            }

            // - Number/any-number => NumberFormat
            // - DateTime/DateTimeOffset/any-datetime/offset => DateTimeFormat
            // - other -> String.format

            if (valueType.isNumber()
                    || anyValue instanceof Number)
            {
                NumberFormat numberFormat = constantNumberFormat;

                if (numberFormat == null)
                {
                    if (constantLocale != null
                            && constantFormat != null)
                    {
                        constantNumberFormat = new DecimalFormat(constantFormat, DecimalFormatSymbols.getInstance(LocaleUtils.toLocale(constantLocale)));
                        numberFormat = constantNumberFormat;
                    }
                    else if (constantFormat != null
                            && !hasLocale)
                    {
                        constantNumberFormat = new DecimalFormat(constantFormat);
                        numberFormat = constantNumberFormat;
                    }
                    else if (hasLocale)
                    {
                        numberFormat = new DecimalFormat(format.valueAsString(i), DecimalFormatSymbols.getInstance(LocaleUtils.toLocale(locale.valueAsString(i))));
                    }
                    else
                    {
                        numberFormat = new DecimalFormat(format.valueAsString(i));
                    }
                }

                if (valueType == Type.Int
                        || valueType == Type.Long)
                {
                    builder.put(UTF8String.from(numberFormat.format(value.getLong(rowCount))));
                }
                else if (valueType == Type.Float
                        || valueType == Type.Double)
                {
                    builder.put(UTF8String.from(numberFormat.format(value.getDouble(rowCount))));
                }
                else if (valueType == Type.Decimal)
                {
                    builder.put(UTF8String.from(numberFormat.format(value.getDecimal(rowCount)
                            .asBigDecimal())));
                }
                else
                {
                    builder.put(UTF8String.from(numberFormat.format(anyValue)));
                }

                continue;
            }
            // Date time formatter
            else if (valueType == Type.DateTime
                    || valueType == Type.DateTimeOffset
                    || isDateTime(anyValue))
            {
                DateTimeFormatter dateTimeFormatter = constantDateTimeFormatter;
                if (dateTimeFormatter == null)
                {
                    if (constantLocale != null
                            && constantFormat != null)
                    {
                        constantDateTimeFormatter = DateTimeFormatter.ofPattern(constantFormat, LocaleUtils.toLocale(constantLocale));
                        dateTimeFormatter = constantDateTimeFormatter;
                    }
                    else if (constantFormat != null
                            && !hasLocale)
                    {
                        constantDateTimeFormatter = DateTimeFormatter.ofPattern(constantFormat);
                        dateTimeFormatter = constantDateTimeFormatter;
                    }
                    else if (hasLocale)
                    {
                        dateTimeFormatter = DateTimeFormatter.ofPattern(format.valueAsString(i), LocaleUtils.toLocale(locale.valueAsString(i)));
                    }
                    else
                    {
                        dateTimeFormatter = DateTimeFormatter.ofPattern(format.valueAsString(i));
                    }
                }

                if (valueType == Type.DateTime)
                {
                    builder.put(UTF8String.from(dateTimeFormatter.format(value.getDateTime(i)
                            .getLocalDateTime())));
                }
                else if (valueType == Type.DateTimeOffset)
                {
                    builder.put(UTF8String.from(dateTimeFormatter.format(value.getDateTimeOffset(i)
                            .getZonedDateTime())));
                }
                else if (anyValue instanceof EpochDateTime)
                {
                    builder.put(UTF8String.from(dateTimeFormatter.format(((EpochDateTime) anyValue).getLocalDateTime())));
                }
                else if (anyValue instanceof EpochDateTimeOffset)
                {
                    builder.put(UTF8String.from(dateTimeFormatter.format(((EpochDateTimeOffset) anyValue).getZonedDateTime())));
                }
                else
                {
                    builder.put(UTF8String.from(dateTimeFormatter.format((Temporal) anyValue)));
                }

                continue;
            }

            // String format
            // bools etc.
            if (hasLocale)
            {
                builder.put(UTF8String.from(String.format(LocaleUtils.toLocale(locale.valueAsString(i)), format.valueAsString(i), value.valueAsObject(i))));
            }
            else
            {
                builder.put(UTF8String.from(String.format(format.valueAsString(i), value.valueAsObject(i))));
            }
        }

        return builder.build();
    }

    private boolean isDateTime(Object value)
    {
        return value instanceof EpochDateTime
                || value instanceof EpochDateTimeOffset
                || value instanceof Temporal;
    }
}
