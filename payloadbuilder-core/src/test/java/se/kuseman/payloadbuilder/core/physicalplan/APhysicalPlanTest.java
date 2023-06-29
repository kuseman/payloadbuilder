package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.Option;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.expression.AExpressionTest;

/** Base class for physical plan tests */
public abstract class APhysicalPlanTest extends AExpressionTest
{
    /** Table source reference used in {@link #scan(IDatasource)} */
    protected final TableSourceReference table = new TableSourceReference("", QualifiedName.of("table"), "t");
    /** Table source reference used in {@link #scan(IDatasource)} */
    protected final TableSourceReference tableB = new TableSourceReference("", QualifiedName.of("tableB"), "b");

    protected final CatalogRegistry catalogRegistry = new CatalogRegistry();
    protected final QuerySession session = new QuerySession(catalogRegistry);
    protected final ExecutionContext context = new ExecutionContext(session);

    protected SortItem sortItem(IExpression expression, Order order, NullOrder nullOrder)
    {
        return new SortItem(expression, order, nullOrder, null);
    }

    TupleIterator ti(Runnable closeAction, TupleVector... vectors)
    {
        return new TupleIterator()
        {
            int index = 0;

            @Override
            public TupleVector next()
            {
                if (!hasNext())
                {
                    throw new NoSuchElementException();
                }

                return vectors[index++];
            }

            @Override
            public boolean hasNext()
            {
                return index < vectors.length;
            }

            @Override
            public void close()
            {
                closeAction.run();
                index = 0;
            }
        };
    }

    /** Create a datasource with schema from vectors. */
    protected IDatasource schemaDS(Runnable closeAction, TupleVector... vectors)
    {
        Schema schema = Schema.EMPTY;
        if (vectors.length > 0)
        {
            schema = vectors[0].getSchema();
            for (int i = 1; i < vectors.length; i++)
            {
                if (!schema.equals(vectors[i].getSchema()))
                {
                    throw new IllegalArgumentException("All vectors schema must be equal");
                }
            }
        }
        return datasource(closeAction, schema, () -> vectors);
    }

    protected TableScan scan(IDatasource ds, TableSourceReference tableSource, Schema schema)
    {
        return new TableScan(0, schema, tableSource, "", false, ds, emptyList());
    }

    protected TableScan scan(IDatasource ds, TableSourceReference tableSource, Schema schema, int batchSize)
    {
        List<Option> options = asList(new Option(QualifiedName.of(DatasourceOptions.BATCH_SIZE), intLit(batchSize)));
        return new TableScan(0, schema, tableSource, "", false, ds, options);
    }

    /** Create a schema less datasource with provided vectors. */
    protected IDatasource schemaLessDS(Runnable closeAction, TupleVector... vectors)
    {
        return datasource(closeAction, Schema.EMPTY, () -> vectors);
    }

    /** Create a schema less datasource with provided vectors. */
    protected IDatasource schemaLessDS(Runnable closeAction, Supplier<TupleVector[]> vectors)
    {
        return datasource(closeAction, Schema.EMPTY, vectors);
    }

    /** Create a schema less datasource with provided vectors. */
    protected IDatasource schemaLessDS(Runnable closeAction, Function<IExecutionContext, TupleVector[]> vectors)
    {
        return datasource(closeAction, Schema.EMPTY, vectors);
    }

    private IDatasource datasource(Runnable closeAction, Schema schema, Supplier<TupleVector[]> vectors)
    {
        return datasource(closeAction, schema, ctx -> vectors.get());
    }

    private IDatasource datasource(Runnable closeAction, Schema schema, Function<IExecutionContext, TupleVector[]> vectors)
    {
        return new IDatasource()
        {
            @Override
            public TupleIterator execute(IExecutionContext context, IDatasourceOptions options)
            {
                return ti(closeAction, vectors.apply(context));
            }
        };
    }
}
