package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;

/** Table function */
public class TableFunction extends TableSource
{
    private final String catalogAlias;
    private final TableFunctionInfo functionInfo;
    private final List<Expression> arguments;

    public TableFunction(String catalogAlias, TableAlias tableAlias, TableFunctionInfo functionInfo, List<Expression> arguments, List<Option> options, Token token)
    {
        super(tableAlias, options, token);
        this.catalogAlias = catalogAlias;
        this.functionInfo = requireNonNull(functionInfo, "functionInfo");
        this.arguments = requireNonNull(arguments, "arguments");
    }

    @Override
    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    public TableFunctionInfo getFunctionInfo()
    {
        return functionInfo;
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
        return isBlank(catalogAlias) ? ""
                : (catalogAlias + "#") + functionInfo.getName()
                  + "("
                  + arguments.stream()
                          .map(a -> a.toString())
                          .collect(joining(", "))
                  + ") "
                  + tableAlias.getAlias();
    }
}
