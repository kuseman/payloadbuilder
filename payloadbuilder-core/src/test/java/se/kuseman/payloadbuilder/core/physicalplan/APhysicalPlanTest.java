package se.kuseman.payloadbuilder.core.physicalplan;

import static java.util.Arrays.asList;
import static java.util.Collections.emptyList;

import java.util.Arrays;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.function.Function;
import java.util.function.Supplier;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.catalog.TableSourceReference;
import se.kuseman.payloadbuilder.core.common.SortItem;
import se.kuseman.payloadbuilder.core.execution.ExecutionContext;
import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.expression.AExpressionTest;

/** Base class for physical plan tests */
public abstract class APhysicalPlanTest extends AExpressionTest
{
    /** Table source reference used in {@link #scan(IDatasource)} */
    protected final TableSourceReference table = new TableSourceReference(0, TableSourceReference.Type.TABLE, "", QualifiedName.of("table"), "t");
    /** Table source reference used in {@link #scan(IDatasource)} */
    protected final TableSourceReference tableB = new TableSourceReference(1, TableSourceReference.Type.TABLE, "", QualifiedName.of("tableB"), "b");

    protected final CatalogRegistry catalogRegistry = new CatalogRegistry();
    protected final QuerySession session = new QuerySession(catalogRegistry);
    protected final ExecutionContext context = new ExecutionContext(session);

    protected SortItem sortItem(IExpression expression, Order order, NullOrder nullOrder)
    {
        return new SortItem(expression, order, nullOrder, null);
    }

    TupleIterator ti(Runnable closeAction, TupleVector... vectors)
    {
        return ti(closeAction, false, vectors);
    }

    TupleIterator ti(Runnable closeAction, boolean unknownEstimateSizes, TupleVector... vectors)
    {
        return new TupleIterator()
        {
            int index = 0;

            @Override
            public int estimatedBatchCount()
            {
                return unknownEstimateSizes ? -1
                        : vectors.length;
            }

            @Override
            public int estimatedRowCount()
            {
                if (unknownEstimateSizes)
                {
                    return -1;
                }
                return Arrays.stream(vectors)
                        .mapToInt(TupleVector::getRowCount)
                        .sum();
            }

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
            }
        };
    }

    /** Create a datasource with schema from vectors. */
    protected IDatasource schemaDS(Runnable closeAction, TupleVector... vectors)
    {
        return schemaDS(closeAction, false, vectors);
    }

    /** Create a datasource with schema from vectors. */
    protected IDatasource schemaDS(Runnable closeAction, boolean unknownEstimateSizes, TupleVector... vectors)
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
        return datasource(closeAction, schema, () -> vectors, unknownEstimateSizes);
    }

    protected IPhysicalPlan scanVectors(IDatasource ds, Schema schema)
    {
        return scanVectors(ds, schema, null);
    }

    protected IPhysicalPlan scanVectors(IDatasource ds, Schema schema, Runnable executeAction)
    {
        return new IPhysicalPlan()
        {
            @Override
            public Schema getSchema()
            {
                return schema;
            }

            @Override
            public int getNodeId()
            {
                return 0;
            }

            @Override
            public List<IPhysicalPlan> getChildren()
            {
                return emptyList();
            }

            @Override
            public <T, C> T accept(IPhysicalPlanVisitor<T, C> visitor, C context)
            {
                throw new RuntimeException("Cannot be visited");
            }

            @Override
            public TupleIterator execute(IExecutionContext context)
            {
                if (executeAction != null)
                {
                    executeAction.run();
                }
                return ds.execute(context);
            }
        };
    }

    protected IPhysicalPlan scan(IDatasource ds, TableSourceReference tableSource, Schema schema)
    {
        return new TableScan(0, schema, tableSource, "", ds, emptyList());
    }

    protected TableScan scan(IDatasource ds, TableSourceReference tableSource, Schema schema, int batchSize)
    {
        List<Option> options = asList(new Option(QualifiedName.of(IExecutionContext.BATCH_SIZE), intLit(batchSize)));
        return new TableScan(0, schema, tableSource, "", ds, options);
    }

    /** Create a schema less datasource with provided vectors. */
    protected IDatasource schemaLessDS(Runnable closeAction, boolean unknownEstiamteSizes, TupleVector... vectors)
    {
        return datasource(closeAction, Schema.EMPTY, () -> vectors, unknownEstiamteSizes);
    }

    /** Create a schema less datasource with provided vectors. */
    protected IDatasource schemaLessDS(Runnable closeAction, TupleVector... vectors)
    {
        return datasource(closeAction, Schema.EMPTY, () -> vectors, false);
    }

    /** Create a schema less datasource with provided vectors. */
    protected IDatasource schemaLessDS(Runnable closeAction, Supplier<TupleVector[]> vectors)
    {
        return datasource(closeAction, Schema.EMPTY, vectors, false);
    }

    /** Create a schema less datasource with provided vectors. */
    protected IDatasource schemaLessDS(Runnable closeAction, Function<IExecutionContext, TupleVector[]> vectors)
    {
        return datasource(closeAction, Schema.EMPTY, vectors, false);
    }

    private IDatasource datasource(Runnable closeAction, Schema schema, Supplier<TupleVector[]> vectors, boolean unknownEstiamteSizes)
    {
        return datasource(closeAction, schema, ctx -> vectors.get(), unknownEstiamteSizes);
    }

    private IDatasource datasource(Runnable closeAction, Schema schema, Function<IExecutionContext, TupleVector[]> vectors, boolean unknownEstiamteSizes)
    {
        return new IDatasource()
        {
            @Override
            public TupleIterator execute(IExecutionContext context)
            {
                return ti(closeAction, unknownEstiamteSizes, vectors.apply(context));
            }
        };
    }
}
