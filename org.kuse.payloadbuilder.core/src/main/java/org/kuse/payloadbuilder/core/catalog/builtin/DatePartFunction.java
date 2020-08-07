package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.time.temporal.ChronoField;
import java.time.temporal.TemporalAccessor;
import java.util.Arrays;
import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

/** Returns date part from input */
class DatePartFunction extends ScalarFunctionInfo
{
    DatePartFunction(Catalog catalog)
    {
        super(catalog, "datepart", Type.SCALAR);
    }

    @Override
    public String getDescription()
    {
        return "Returns date part from provided Date. " + System.lineSeparator() +
            "Valid parts are: " + System.lineSeparator() +
            Arrays.stream(Part.values())
                    .filter(p -> p.abbreviationFor == null)
                    .map(p ->
                    {
                        String name = p.name();
                        List<Part> abbreviations = Arrays.stream(Part.values()).filter(pp -> pp.abbreviationFor == p).collect(toList());
                        if (abbreviations.isEmpty())
                        {
                            return name;
                        }

                        return name + " ( Abbreviations: " + abbreviations.toString() + " )";
                    })
                    .collect(joining(System.lineSeparator()))
            + System.lineSeparator() +
            "Ex. datepart(datepartExpression, dateExpression) ";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class);
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object value = arguments.get(1).eval(context);
        if (value == null)
        {
            return null;
        }

        String partString;
        Expression partExpression = arguments.get(0);
        if (partExpression instanceof QualifiedReferenceExpression)
        {
            partString = ((QualifiedReferenceExpression) partExpression).getQname().toString();
        }
        else
        {
            Object obj = partExpression.eval(context);
            if (obj == null)
            {
                return null;
            }
            partString = String.valueOf(obj);
        }

        if (!(value instanceof TemporalAccessor))
        {
            throw new IllegalArgumentException("Expected a valid datetime value for " + getName() + " but got: " + value);
        }

        TemporalAccessor temporal = (TemporalAccessor) value;
        Part part = Part.valueOf(partString.toUpperCase());
        return temporal.get(part.getChronoField());
    }

    enum Part
    {
        YEAR(ChronoField.YEAR),
        YY(YEAR),
        YYYY(YEAR),

        //        quarter qq, q
        MONTH(ChronoField.MONTH_OF_YEAR),
        MM(MONTH),
        M(MONTH),

        DAYOFYEAR(ChronoField.DAY_OF_YEAR),
        DY(DAYOFYEAR),
        Y(DAYOFYEAR),

        DAY(ChronoField.DAY_OF_MONTH),
        DD(DAY),
        D(DAY),

        WEEK(ChronoField.ALIGNED_WEEK_OF_YEAR),
        WK(WEEK),
        WW(WEEK),

        WEEKDAY(ChronoField.DAY_OF_WEEK),
        DW(WEEKDAY),

        HOUR(ChronoField.HOUR_OF_DAY),
        HH(HOUR),

        MINUTE(ChronoField.MINUTE_OF_HOUR),
        MI(MINUTE),
        N(MINUTE),

        SECOND(ChronoField.SECOND_OF_MINUTE),
        SS(SECOND),
        S(SECOND),

        MILLISECOND(ChronoField.MILLI_OF_SECOND),
        MS(MILLISECOND),

        MICROSECOND(ChronoField.MICRO_OF_SECOND),
        MCS(MICROSECOND),

        NANOSECOND(ChronoField.NANO_OF_SECOND),
        NS(NANOSECOND);

        ChronoField chronoField;
        Part abbreviationFor;

        Part(Part abbreviationFor)
        {
            this.abbreviationFor = abbreviationFor;
        }

        Part(ChronoField chronoField)
        {
            this.chronoField = chronoField;
        }
        
        ChronoField getChronoField()
        {
            return chronoField != null ? chronoField : abbreviationFor.chronoField;
        }
    }
}
