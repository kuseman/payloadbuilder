package com.viskan.payloadbuilder.parser;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.FunctionInfo;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

import org.antlr.v4.runtime.Token;

/** Scalar function call */
public class QualifiedFunctionCallExpression extends Expression
{
    private final QualifiedName qname;
    
//    private final ScalarFunctionInfo functionInfo;
    private final List<Expression> arguments;
    
    /** Unique id for this function call in query.
     * Used to store function lookup during evaluation to avoid multiple lookups */
    private final int functionId;

    private final Token token;

    QualifiedFunctionCallExpression(
            QualifiedName qname,
            List<Expression> arguments,
            int functionId)
    {
        this(qname, arguments, functionId, null);
    }
    
    QualifiedFunctionCallExpression(
            QualifiedName qname,
            List<Expression> arguments,
            int functionId,
            Token token)
    {
        this.qname = requireNonNull(qname, "qname");
        this.arguments = requireNonNull(arguments, "arguments");
        this.functionId = functionId;
        this.token = token;
    }

    public QualifiedName getQname()
    {
        return qname;
    }

    public List<Expression> getArguments()
    {
        return arguments;
    }
    
    /** Resolves function this expression based on provided session */
    public ScalarFunctionInfo getFunctionInfo(QuerySession session)
    {
        FunctionInfo functionInfo = session.resolveFunctionInfo(qname, functionId);
        
        if (!(functionInfo instanceof ScalarFunctionInfo))
        {
            throw new ParseException("Expected a scalar function but got " + functionInfo, token);
        }
        
        return (ScalarFunctionInfo) functionInfo;
    
        
        /*
         * QuerySession (Static data through whole execution, exception of properties)
         *   Catalogs
         *   Params
         * 
         * OperatorContext (Context used during selects)
         *   NodeDat
         *   
         * 
         * EvaluationContext (Context used during evaluation)
         *   LambdaValues
         * 
         * 
         * ExecutionContext
         *   QuerySession
         *   EvaluationContext (cleared before each evalution)
         * 
         */
        
        
//        throw new IllegalArgumentException("No function named: " + qname.getLast() + " found in " + qname.getCatalog());
//        return null;
    }

    @Override
    public Object eval(ExecutionContext context)
    {
        ScalarFunctionInfo functionInfo = getFunctionInfo(context.getSession());
        return functionInfo.eval(context, arguments);
    }

//    @Override
//    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
//    {
//        return null;//functionInfo.generateCode(context, parentCode, arguments);
//    }

    @Override
    public <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public Class<?> getDataType()
    {
        return Object.class;// functionInfo.getDataType();
    }

    @Override
    public boolean isNullable()
    {
        return true;//functionInfo.isNullable();
    }

    @Override
    public int hashCode()
    {
        return 17 +
            (37 * qname.hashCode()) +
            (37 * arguments.hashCode());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof QualifiedFunctionCallExpression)
        {
            QualifiedFunctionCallExpression that = (QualifiedFunctionCallExpression) obj;
            return qname.equals(that.qname)
                && arguments.equals(that.arguments);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return qname + "(" + arguments.stream().map(a -> a.toString()).collect(joining(", ")) + ")";
    }
}
