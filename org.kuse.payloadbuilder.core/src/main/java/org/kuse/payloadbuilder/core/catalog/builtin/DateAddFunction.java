package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.Arrays.asList;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;

import java.time.temporal.Temporal;
import java.util.Arrays;
import java.util.List;

import org.apache.commons.lang3.EnumUtils;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.catalog.builtin.DatePartFunction.Part;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.LiteralStringExpression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

/** DateAdd */
class DateAddFunction extends ScalarFunctionInfo
{
    DateAddFunction(Catalog catalog)
    {
        super(catalog, "dateadd");
    }

    @Override
    public String getDescription()
    {
        return "Adds a number for a specific date part to provided Date. " + System.lineSeparator() +
            "Valid parts are: " + System.lineSeparator() +
            Arrays.stream(DatePartFunction.Part.values())
                    .filter(p -> p.abbreviationFor == null)
                    .map(p ->
                    {
                        String name = p.name();
                        List<DatePartFunction.Part> abbreviations = Arrays.stream(DatePartFunction.Part.values()).filter(pp -> pp.abbreviationFor == p).collect(toList());
                        if (abbreviations.isEmpty())
                        {
                            return name;
                        }

                        return name + " ( Abbreviations: " + abbreviations.toString() + " )";
                    })
                    .collect(joining(System.lineSeparator()))
            + System.lineSeparator() +
            "Ex. dateadd(datepartExpression, integerExpression, dateExpression) ";
    }

    @Override
    public List<Class<? extends Expression>> getInputTypes()
    {
        return asList(Expression.class, Expression.class, Expression.class);
    }
    
    /** Convert Part argument from QRES to Strings if they match DateType enum */
    @Override
    public List<Expression> foldArguments(List<Expression> arguments)
    {
        if (arguments.get(0) instanceof QualifiedReferenceExpression)
        {
            QualifiedReferenceExpression qre = (QualifiedReferenceExpression) arguments.get(0);
            if (qre.getLambdaId() >= 0)
            {
                return arguments;
            }
            Part part = EnumUtils.getEnumIgnoreCase(Part.class, qre.getQname().toString().toUpperCase());
            if (part != null)
            {
                arguments.set(0, new LiteralStringExpression(part.name()));
            }
        }
        return arguments;
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        Object value = arguments.get(2).eval(context);
        if (value == null)
        {
            return null;
        }

        Object obj = arguments.get(0).eval(context);
        if (obj == null)
        {
            return null;
        }
        String partString = String.valueOf(obj);
        Object numberObj = arguments.get(1).eval(context);
        if (!(numberObj instanceof Integer))
        {
            throw new IllegalArgumentException("Expected a integer expression for " + getName() + " but got: " + numberObj);
        }
        int number = ((Integer) numberObj).intValue();
        if (!(value instanceof Temporal))
        {
            throw new IllegalArgumentException("Expected a valid datetime value for " + getName() + " but got: " + value);
        }

        Temporal temporal = (Temporal) value;
        DatePartFunction.Part part = DatePartFunction.Part.valueOf(partString.toUpperCase());
        return temporal.plus(number, part.getChronoField().getBaseUnit());
    }
}
