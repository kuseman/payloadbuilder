package se.kuseman.payloadbuilder.api.expression;

import java.time.temporal.ChronoField;

/** Datepart function */
public interface IDatePartExpression extends IUnaryExpression
{
    /** Return part */
    Part getPart();

    @Override
    default <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    /** Date parts */
    public enum Part
    {
        YEAR(ChronoField.YEAR),
        YY(YEAR),
        YYYY(YEAR),

        // quarter qq, q
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

        final ChronoField chronoField;
        final Part abbreviationFor;

        Part(Part abbreviationFor)
        {
            this.abbreviationFor = abbreviationFor;
            this.chronoField = null;
        }

        Part(ChronoField chronoField)
        {
            this.abbreviationFor = null;
            this.chronoField = chronoField;
        }

        public ChronoField getChronoField()
        {
            return chronoField != null ? chronoField
                    : abbreviationFor.chronoField;
        }
    }
}
