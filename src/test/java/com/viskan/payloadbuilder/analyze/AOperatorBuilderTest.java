package com.viskan.payloadbuilder.analyze;

import com.viskan.payloadbuilder.Row;
import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.CatalogRegistry;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.operator.OperatorContext;
import com.viskan.payloadbuilder.operator.Projection;
import com.viskan.payloadbuilder.parser.QueryParser;
import com.viskan.payloadbuilder.parser.tree.Expression;

import static java.util.Collections.emptyIterator;

import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;

import org.apache.commons.lang3.mutable.MutableObject;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.Assert;

/** Base class of {@link OperatorBuilder} tests. */
public class AOperatorBuilderTest extends Assert
{
    protected final QueryParser parser = new QueryParser();
    protected final CatalogRegistry catalogRegistry = new CatalogRegistry();

    protected Expression e(String expression)
    {
        return parser.parseExpression(catalogRegistry, expression);
    }

    protected QueryResult getQueryResult(String query)
    {
        List<Operator> tableOperators = new ArrayList<>();
        MutableObject<TableAlias> mAlias = new MutableObject<>();
        Catalog c = new Catalog("Test")
        {
            @Override
            public Operator getScanOperator(TableAlias alias)
            {
                if (mAlias.getValue() == null)
                {
                    mAlias.setValue(alias);
                }
                Operator op = new Operator()
                {
                    @Override
                    public Iterator<Row> open(OperatorContext context)
                    {
                        return emptyIterator();
                    }

                    @Override
                    public String toString()
                    {
                        return alias.getTable().toString();
                    }
                };

                tableOperators.add(op);
                return op;
            }
        };

        catalogRegistry.setDefaultCatalog(c);
        Pair<Operator, Projection> pair = OperatorBuilder.create(catalogRegistry, parser.parseQuery(catalogRegistry, query));

        QueryResult result = new QueryResult();

        if (mAlias.getValue() != null)
        {
            TableAlias alias = mAlias.getValue();
            while (alias.getParent() != null)
            {
                alias = alias.getParent();
            }
            result.alias = alias;
        }
        result.operator = pair.getLeft();
        result.projection = pair.getRight();
        result.tableOperators = tableOperators;
        return result;
    }

    static class QueryResult
    {
        List<Operator> tableOperators;

        Operator operator;
        Projection projection;
        TableAlias alias;
    }
}
