package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.util.List;

import org.antlr.v4.runtime.Token;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.common.Option;

/** A table function scan in the logical plan */
public class TableFunctionScan extends TableSource
{
    private final TableSourceReference tableSource;
    private final List<IExpression> arguments;
    private final Schema schema;
    private final Token token;
    private final List<Option> options;

    public TableFunctionScan(TableSourceReference tableSource, Schema schema, List<IExpression> arguments, List<Option> options, Token token)
    {
        super(requireNonNull(tableSource, "tableSource").getCatalogAlias(), tableSource.getAlias());
        this.tableSource = tableSource;
        this.schema = requireNonNull(schema, "schema");
        this.arguments = requireNonNull(arguments, "arguments");
        this.options = requireNonNull(options, "options");
        this.token = token;
    }

    public TableSourceReference getTableSource()
    {
        return tableSource;
    }

    public List<IExpression> getArguments()
    {
        return arguments;
    }

    public List<Option> getOptions()
    {
        return options;
    }

    public Token getToken()
    {
        return token;
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public <T, C> T accept(ILogicalPlanVisitor<T, C> visitor, C context)
    {
        return visitor.visit(this, context);
    }

    @Override
    public int hashCode()
    {
        return tableSource.hashCode();
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TableFunctionScan)
        {
            TableFunctionScan that = (TableFunctionScan) obj;
            return super.equals(that)
                    && tableSource.equals(that.tableSource)
                    && arguments.equals(that.arguments)
                    && options.equals(that.options);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return "Function scan: " + tableSource.toString()
               + "("
               + arguments.stream()
                       .map(IExpression::toVerboseString)
                       .collect(joining(", "))
               + ") "
               + tableSource.getAlias();
    }
}
