package org.kuse.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.kuse.payloadbuilder.core.catalog.FunctionInfo.validate;

import java.util.List;

import org.antlr.v4.runtime.Token;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.FunctionInfo;
import org.kuse.payloadbuilder.core.catalog.TableFunctionInfo;

/** Table function */
public class TableFunction extends TableSource
{
    private final String catalog;
    private final String function;
    private final List<Expression> arguments;
    private final int functionId;
    private final List<TableOption> tableOptions;

    public TableFunction(String catalog, String function, List<Expression> arguments, String alias, List<TableOption> tableOptions, int functionId, Token token)
    {
        super(alias, token);
        this.catalog = catalog;
        this.function = requireNonNull(function, "function");
        this.arguments = requireNonNull(arguments, "arguments");
        this.tableOptions = requireNonNull(tableOptions, "tableOptions");
        this.functionId = functionId;
    }
    
    @Override
    public String getCatalog()
    {
        return catalog;
    }
    
    public String getFunction()
    {
        return function;
    }
    
    @Override
    public List<TableOption> getTableOptions()
    {
        return tableOptions;
    }
    
    public TableFunctionInfo getFunctionInfo(QuerySession session)
    {
        FunctionInfo functionInfo = session.resolveFunctionInfo(catalog, function, functionId);
        
        if (functionInfo == null)
        {
            throw new ParseException("No function found with name " + function, token);
        }
        else if (!(functionInfo instanceof TableFunctionInfo))
        {
            throw new ParseException("Expected a table valued function but got " + functionInfo, token);
        }
        
        validate(functionInfo, arguments, token);
        
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
        return catalog != null ? (catalog + "#")
            : ""
                + function
                + "(" + arguments.stream().map(a -> a.toString()).collect(joining(", ")) + ") " + alias;
    }
}
