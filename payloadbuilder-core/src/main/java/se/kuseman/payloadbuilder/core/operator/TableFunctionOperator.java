package se.kuseman.payloadbuilder.core.operator;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;
import static java.util.stream.Collectors.toList;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.entry;
import static se.kuseman.payloadbuilder.api.utils.MapUtils.ofEntries;

import java.util.List;
import java.util.Map;

import org.apache.commons.lang3.StringUtils;

import se.kuseman.payloadbuilder.api.TableAlias;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.operator.AOperator;
import se.kuseman.payloadbuilder.api.operator.IExecutionContext;
import se.kuseman.payloadbuilder.core.parser.Expression;

/** Operator handling TVF's */
class TableFunctionOperator extends AOperator
{
    private final String catalogAlias;
    private final TableAlias tableAlias;
    private final TableFunctionInfo functionInfo;
    private final List<Expression> arguments;

    TableFunctionOperator(int nodeId, String catalogAlias, TableAlias tableAlias, TableFunctionInfo functionInfo, List<Expression> arguments)
    {
        super(nodeId);
        this.catalogAlias = catalogAlias;
        this.tableAlias = requireNonNull(tableAlias, "tableAlias");
        this.functionInfo = requireNonNull(functionInfo, "tableFunction");
        this.arguments = requireNonNull(arguments, "arguments");
    }

    @Override
    public String getName()
    {
        return "Function: " + functionInfo.getCatalog()
                .getName()
               + "#"
               + functionInfo.getName();
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        return ofEntries(true, entry(CATALOG, functionInfo.getCatalog()
                .getName()), entry("Arguments",
                        arguments.stream()
                                .map(Object::toString)
                                .collect(toList())));
    }

    @Override
    public TupleIterator open(IExecutionContext context)
    {
        String currentAlias = catalogAlias;
        if (StringUtils.isBlank(currentAlias))
        {
            // Blank then use the current default alias
            currentAlias = context.getSession()
                    .getDefaultCatalogAlias();
        }
        return functionInfo.open(context, currentAlias, tableAlias, arguments);
    }

    @Override
    public int hashCode()
    {
        // CSOFF
        int hashCode = 17;
        hashCode = hashCode * 37 + functionInfo.hashCode();
        hashCode = hashCode * 37 + arguments.hashCode();
        return hashCode;
        // CSON
    }

    @Override
    public boolean equals(Object obj)
    {
        if (obj instanceof TableFunctionOperator)
        {
            TableFunctionOperator that = (TableFunctionOperator) obj;
            return nodeId == that.nodeId
                    && tableAlias.isEqual(that.tableAlias)
                    && functionInfo.equals(that.functionInfo)
                    && arguments.equals(that.arguments);
        }
        return false;
    }

    @Override
    public String toString()
    {
        return String.format("%s (ID: %d, %s)", functionInfo.getName(), nodeId, arguments.stream()
                .map(Object::toString)
                .collect(joining(", ")));
    }
}
