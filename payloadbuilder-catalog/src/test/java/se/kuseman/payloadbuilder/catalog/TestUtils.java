package se.kuseman.payloadbuilder.catalog;

import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

import java.time.Duration;
import java.util.Map;
import java.util.function.Supplier;

import org.mockito.Mockito;
import org.mockito.invocation.InvocationOnMock;
import org.mockito.stubbing.Answer;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ISortItem;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.NullOrder;
import se.kuseman.payloadbuilder.api.catalog.ISortItem.Order;
import se.kuseman.payloadbuilder.api.execution.GenericCache;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.IStatementContext;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** Common test utils for catalog tests */
public final class TestUtils
{

    public static ISortItem mockSortItem(QualifiedName qname)
    {
        return mockSortItem(qname, Order.ASC);
    }

    public static ISortItem mockSortItem(QualifiedName qname, Order order)
    {
        return mockSortItem(qname, order, NullOrder.UNDEFINED);
    }

    /** Mock sort item */
    public static ISortItem mockSortItem(QualifiedName qname, Order order, NullOrder nulLOrder)
    {
        ISortItem item = mock(ISortItem.class);
        IExpression exp = mock(IExpression.class);
        when(exp.getQualifiedColumn()).thenReturn(qname);
        when(item.getExpression()).thenReturn(exp);
        when(item.getOrder()).thenReturn(order);
        when(item.getNullOrder()).thenReturn(nulLOrder);
        return item;
    }

    /** Mock {@link IExecutionContext} with provided data */
    @SuppressWarnings("unchecked")
    public static IExecutionContext mockExecutionContext(String catalogAlias, Map<String, Object> properties, int nodeId, NodeData data)
    {
        IExecutionContext context = Mockito.mock(IExecutionContext.class);
        IQuerySession session = Mockito.mock(IQuerySession.class);
        IStatementContext statementContext = Mockito.mock(IStatementContext.class);

        when(context.getSession()).thenReturn(session);
        when(context.getStatementContext()).thenReturn(statementContext);
        when(statementContext.getOrCreateNodeData(eq(nodeId), any(Supplier.class))).thenReturn(data);

        when(session.getGenericCache()).thenReturn(new GenericCache()
        {
            @Override
            public <T> T computIfAbsent(QualifiedName name, Object key, Duration ttl, Supplier<T> supplier)
            {
                return supplier.get();
            }
        });

        when(session.getCatalogProperty(eq(catalogAlias), anyString(), any())).thenCallRealMethod();
        when(session.getCatalogProperty(eq(catalogAlias), anyString())).thenAnswer(new Answer<Object>()
        {
            @Override
            public Object answer(InvocationOnMock invocation) throws Throwable
            {
                String key = invocation.getArgument(1);
                return ValueVector.literalAny(properties.get(key));
            }
        });

        return context;
    }

    /** Mock {@link IDatasourceOptions} */
    public static IDatasourceOptions mockOptions(int batchSize)
    {
        IDatasourceOptions options = Mockito.mock(IDatasourceOptions.class);
        when(options.getBatchSize(Mockito.any(IExecutionContext.class))).thenReturn(batchSize);
        return options;
    }
}