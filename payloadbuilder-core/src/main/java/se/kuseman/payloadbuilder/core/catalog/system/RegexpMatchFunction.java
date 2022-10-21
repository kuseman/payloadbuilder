package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.Collections.emptyIterator;

import java.util.Iterator;
import java.util.List;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.ScalarFunctionInfo;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;

/** Regexp match. Matches input with a regular expression and outputs pattern based on matching */
class RegexpMatchFunction extends ScalarFunctionInfo
{
    RegexpMatchFunction(Catalog catalog)
    {
        super(catalog, "regexp_match");
    }

    @Override
    public String getDescription()
    {
        //@formatter:off
        return "Matches first argument to regex provided in second argument." + System.lineSeparator()
               + "Ex. regexp_match(expression, stringExpression [ ,patternExpression])." + System.lineSeparator()
               + "This returns an array of values with matched gorups." + System.lineSeparator()
               + System.lineSeparator()
               + "Returns a list of values.";
        //@formatter:on
    }

    @Override
    public Object eval(IExecutionContext context, String catalogAlias, List<? extends IExpression> arguments)
    {
        Object obj = arguments.get(0)
                .eval(context);

        if (obj == null)
        {
            return null;
        }

        Object patternObj = arguments.get(1)
                .eval(context);
        if (!(patternObj instanceof String))
        {
            throw new IllegalArgumentException("Expected a String pattern for function " + getName() + " but got " + patternObj);
        }

        String value = String.valueOf(obj);
        Pattern pattern = Pattern.compile((String) patternObj);

        final Matcher matcher = pattern.matcher(value);

        if (!matcher.find())
        {
            return emptyIterator();
        }

        final int count = matcher.groupCount();

        if (count == 0)
        {
            return emptyIterator();
        }

        Iterator<Object> it = new Iterator<Object>()
        {
            int currentGroup = 1;
            String next = matcher.group(currentGroup);

            @Override
            public Object next()
            {
                String result = next;
                next = null;
                return result;
            }

            @Override
            public boolean hasNext()
            {
                if (next != null)
                {
                    return true;
                }

                return setNext();
            }

            private boolean setNext()
            {
                currentGroup++;
                if (currentGroup > count)
                {
                    // End of current find, move to next
                    currentGroup = 0;
                    if (!matcher.find())
                    {
                        return false;
                    }
                    return setNext();
                }

                next = matcher.group(currentGroup);
                return true;
            }
        };
        return it;
    }
}
