package org.kuse.payloadbuilder.core.codegen;

import java.util.List;

import org.kuse.payloadbuilder.core.operator.IOrdinalValuesFactory;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Base class for generated instances of {@link IOrdinalValuesFactory} */
//CSOFF
public abstract class BaseOrdinalValuesFactory extends BaseGeneratedClass implements IOrdinalValuesFactory
//CSON
{
    private List<Expression> expressions;

    void setExpressions(List<Expression> expressions)
    {
        this.expressions = expressions;
    }

    public List<Expression> getExpressions()
    {
        return expressions;
    }

    @Override
    public String toString()
    {
        return "Gen: " + expressions.toString();
    }

    /** Base class for generated {@link IOrdinalValues} */
    //CSOFF
    public abstract static class BaseOrdinalValues implements IOrdinalValues
    //CSON
    {
        /* Is overriden inside the generated code */
        @Override
        public abstract int hashCode();

        //CSOFF
        //@formatter:off
        @Override
        public boolean equals(Object obj)
        {
            if (!(obj instanceof IOrdinalValues))
            {
                return false;
            }

            return isEquals((IOrdinalValues) obj);
        }
        //CSON
        //@formatter:on
    }
}
