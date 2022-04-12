package se.kuseman.payloadbuilder.core.parser;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.List;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.TableAlias;

/** Table function */
public class UnresolvedTableFunction extends TableSource
{
    private final String catalogAlias;
    private final String name;
    private final List<Expression> arguments;

    public UnresolvedTableFunction(String catalogAlias, String name, TableAlias tableAlias, List<Expression> arguments, List<Option> options, Token token)
    {
        super(tableAlias, options, token);
        this.catalogAlias = requireNonNull(catalogAlias, "catalogAlias");
        this.name = requireNonNull(name, "name");
        this.arguments = requireNonNull(arguments, "arguments");
    }

    @Override
    public String getCatalogAlias()
    {
        return catalogAlias;
    }

    public String getName()
    {
        return name;
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
                : (catalogAlias + "#") + name
                  + "("
                  + arguments.stream()
                          .map(a -> a.toString())
                          .collect(joining(", "))
                  + ") "
                  + tableAlias.getAlias();
    }
}
