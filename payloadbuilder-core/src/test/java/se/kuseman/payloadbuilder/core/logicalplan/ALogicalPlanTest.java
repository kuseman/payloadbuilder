package se.kuseman.payloadbuilder.core.logicalplan;

import static java.util.Collections.emptyList;

import java.util.List;

import se.kuseman.payloadbuilder.api.catalog.DatasourceData.Projection;
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
        return new TableScan(new TableSchema(schema), tableSource, Projection.ALL, emptyList(), null);
    }

    protected TableScan tableScan(Schema schema, TableSourceReference tableSource, List<String> projection)
    {
        return new TableScan(new TableSchema(schema), tableSource, Projection.columns(projection), emptyList(), null);
    }

    protected TableScan tableScanNoProjection(Schema schema, TableSourceReference tableSource)
    {
        return new TableScan(new TableSchema(schema), tableSource, Projection.NONE, emptyList(), null);
    }

    protected SubQuery subQuery(ILogicalPlan input, TableSourceReference tableSource)
    {
        return new SubQuery(input, tableSource, null);
    }

    protected se.kuseman.payloadbuilder.core.logicalplan.Projection projection(ILogicalPlan input, List<IExpression> expressions)
    {
        return new se.kuseman.payloadbuilder.core.logicalplan.Projection(input, expressions, null);
    }

    protected se.kuseman.payloadbuilder.core.logicalplan.Projection projection(ILogicalPlan input, List<IExpression> expressions, TableSourceReference parentTableSource)
    {
        return new se.kuseman.payloadbuilder.core.logicalplan.Projection(input, expressions, parentTableSource);
    }

    protected SortItem sortItem(IExpression expression, Order order)
    {
        return new SortItem(expression, order, NullOrder.UNDEFINED, null);
    }
}
