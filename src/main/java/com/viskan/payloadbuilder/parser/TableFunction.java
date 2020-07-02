package com.viskan.payloadbuilder.parser;

import com.viskan.payloadbuilder.QuerySession;
import com.viskan.payloadbuilder.catalog.FunctionInfo;
import com.viskan.payloadbuilder.catalog.TableFunctionInfo;

import static java.util.Objects.requireNonNull;

import java.util.List;

import org.antlr.v4.runtime.Token;

/** Table function */
public class TableFunction extends TableSource
{
    private final QualifiedName qname;
//    private final TableFunctionInfo functionInfo;
    private final List<Expression> arguments;
    private final int functionId;
    private final Token token;

    public TableFunction(QualifiedName qname, List<Expression> arguments, String alias, int functionId, Token token)
    {
        super(alias);
        this.qname = requireNonNull(qname, "qname");
//        this.functionInfo = requireNonNull(functionInfo, "functionInfo");
        this.arguments = requireNonNull(arguments, "arguments");
        this.functionId = functionId;
        this.token = token;
    }
    
    public QualifiedName getQname()
    {
        return qname;
    }
    
    public TableFunctionInfo getFunctionInfo(QuerySession session)
    {
        FunctionInfo functionInfo = session.resolveFunctionInfo(qname, functionId);
        
        if (!(functionInfo instanceof TableFunctionInfo))
        {
            throw new ParseException("Expected a table valued function but got " + functionInfo, token);
        }
        
        return (TableFunctionInfo) functionInfo;
    }
    
    public List<Expression> getArguments()
    {
        return arguments;
    }
    
    @Override
    public <TR, TC> TR accept(SelectVisitor<TR, TC> visitor, TC context)
    {
        return visitor.visit(this, context);                
    }
    
    @Override
    public String toString()
    {
        return qname + " " + alias;
    }
}
