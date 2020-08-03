package org.kuse.payloadbuilder.core.catalog.builtin;

import java.util.List;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;

/** Cast and convert function */
class CastFunction extends ScalarFunctionInfo
{
    CastFunction(Catalog catalog, String name)
    {
        super(catalog, name, Type.SCALAR);
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
        BOOLEAN
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
//        DATE,
//        DATETIME,
        DOUBLE
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
        FLOAT
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
        INTEGER
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
        LONG
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
        STRING
        {
            @Override
            Object convert(Object source, int style)
            {
                return String.valueOf(source);
            }
        };
        
        /** Convert provided source */
        abstract Object convert(Object source, int style);
    }
}