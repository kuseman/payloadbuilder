package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.emptyList;

import java.util.List;
import java.util.Optional;

import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.expression.AExpressionTest;

/** Base class for logical plan tests */
public abstract class ALogicalPlanTest extends AExpressionTest
{
    protected TableScan tableScan(Schema schema, TableSourceReference tableSource)
    {
        return new TableScan(new TableSchema(schema), tableSource, Optional.empty(), false, emptyList(), null);
    }

    protected TableScan tableScan(Schema schema, TableSourceReference tableSource, List<String> projection)
    {
        return new TableScan(new TableSchema(schema), tableSource, Optional.of(projection), false, emptyList(), null);
    }

    protected SubQuery subQuery(ILogicalPlan input, String alias)
    {
        return new SubQuery(input, alias, null);
    }

    protected Projection projection(ILogicalPlan input, List<IExpression> expressions)
    {
        return new Projection(input, expressions, false);
    }

    protected Projection compute(ILogicalPlan input, List<IExpression> expressions)
    {
        return new Projection(input, expressions, true);
    }

    protected SortItem sortItem(IExpression expression, Order order)
    {
        return new SortItem(expression, order, NullOrder.UNDEFINED, null);
    }
}
