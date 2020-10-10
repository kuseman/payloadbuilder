package org.kuse.payloadbuilder.catalog.jdbc;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.Index;
import org.kuse.payloadbuilder.core.operator.Operator;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair;
import org.kuse.payloadbuilder.core.operator.PredicateAnalyzer.AnalyzePair.Type;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.QualifiedName;
import org.kuse.payloadbuilder.core.parser.QualifiedReferenceExpression;
import org.kuse.payloadbuilder.core.parser.SortItem;
import org.kuse.payloadbuilder.core.parser.SortItem.NullOrder;

import com.zaxxer.hikari.HikariDataSource;

/** Jdbc catalog */
public class JdbcCatalog extends Catalog
{
    private static final int BATCH_SIZE = 500;
    public static final String NAME = "JdbcCatalog";
    static final String CONNECTION_STRING = "connectionstring";
    static final String USERNAME = "username";
    static final String PASSWORD = "password";
    static final String DATABASE = "database";

    public JdbcCatalog()
    {
        super(NAME);
        registerFunction(new QueryFunction(this));
    }

    @Override
    public List<String> getTables(QuerySession session, String catalogAlias)
    {
        List<String> tables = new ArrayList<>();
        String database = (String) session.getCatalogProperty(catalogAlias, JdbcCatalog.DATABASE);
        try (Connection connection = getConnection(session, catalogAlias);
                ResultSet rs = connection.getMetaData().getTables(database, null, null, null);)
        {
            while (rs.next())
            {
                tables.add(rs.getString(3));
            }
        }
        catch (SQLException e)
        {
            throw new RuntimeException("Error listing tables", e);
        }

        return tables;
    }

    @Override
    public List<Index> getIndices(QuerySession session, String catalogAlias, QualifiedName table)
    {
        return singletonList(new Index(table, Index.ALL_COLUMNS, BATCH_SIZE));
    }

    @Override
    public Operator getScanOperator(OperatorData data)
    {
        List<AnalyzePair> pairs = getPredicatePairs(data);
        List<SortItem> sortItems = getSortItems(data);
        return new JdbcOperator(this, data.getNodeId(), data.getCatalogAlias(), data.getTableAlias(), pairs, sortItems, null);
    }

    @Override
    public Operator getIndexOperator(OperatorData data, Index index)
    {
        List<AnalyzePair> pairs = getPredicatePairs(data);
        List<SortItem> sortItems = getSortItems(data);
        return new JdbcOperator(this, data.getNodeId(), data.getCatalogAlias(), data.getTableAlias(), pairs, sortItems, index);
    }

    private List<AnalyzePair> getPredicatePairs(OperatorData data)
    {
        List<AnalyzePair> pairs = new ArrayList<>();
        if (!data.getPredicatePairs().isEmpty())
        {
            Iterator<AnalyzePair> it = data.getPredicatePairs().iterator();
            while (it.hasNext())
            {
                AnalyzePair pair = it.next();

                if (pair.getType() == Type.UNDEFINED)
                {
                    continue;
                }
                QualifiedName qname = pair.getQname(data.getTableAlias().getAlias());
                if (qname == null || qname.getParts().size() > 2)
                {
                    continue;
                }

                pairs.add(pair);
                it.remove();
            }
        }
        return pairs;
    }

    private List<SortItem> getSortItems(OperatorData data)
    {
        List<SortItem> sortItems = emptyList();
        if (!data.getSortItems().isEmpty()
            && data.getSortItems().stream().allMatch(i -> isApplicableSortItem(data.getTableAlias(), i)))
        {
            sortItems = new ArrayList<>(data.getSortItems());
            data.getSortItems().clear();
        }
        return sortItems;
    }

    private boolean isApplicableSortItem(TableAlias tableAlias, SortItem item)
    {
        if (item.getNullOrder() != NullOrder.UNDEFINED)
        {
            return false;
        }

        if (!(item.getExpression() instanceof QualifiedReferenceExpression))
        {
            return false;
        }

        QualifiedReferenceExpression qre = (QualifiedReferenceExpression) item.getExpression();
        QualifiedName qname = qre.getQname();

        // One part qname or 2 part where alias is matching
        return qname.getParts().size() == 1
            || (qname.getParts().size() == 2 && equalsIgnoreCase(tableAlias.getAlias(), qname.getParts().get(0)));
    }

    private final Map<String, HikariDataSource> dataSourceByUrl = new ConcurrentHashMap<>();

    /** Get connection for provided session/catalog alias */
    Connection getConnection(QuerySession session, String catalogAlias)
    {
        final String connectionString = (String) session.getCatalogProperty(catalogAlias, JdbcCatalog.CONNECTION_STRING);
        if (isBlank(connectionString))
        {
            throw new IllegalArgumentException("Missing connectionstring in catalog properties for " + catalogAlias);
        }

        final String username = (String) session.getCatalogProperty(catalogAlias, JdbcCatalog.USERNAME);
        final String password = getPassword(session, catalogAlias);
        if (isBlank(username) || isBlank(password))
        {
            throw new CredentialsException(catalogAlias, "Missing username/password in catalog properties for " + catalogAlias);
        }

        return getConnection(connectionString, username, password, catalogAlias);
    }

    /** Get connection for provided connection string and user/pass */
    Connection getConnection(String connectionString, String username, String password, String catalogAlias)
    {
        try
        {
            return dataSourceByUrl.compute(connectionString, (k, v) ->
            {
                if (v == null)
                {
                    HikariDataSource ds = new HikariDataSource();
                    ds.setRegisterMbeans(true);
                    //CSOFF
                    ds.setPoolName(connectionString.substring(0, 40));
                    //CSON
                    ds.setJdbcUrl(connectionString);
                    ds.setUsername(username);
                    ds.setPassword(password);
                    return ds;
                }

                // Set user/pass in case they have changed
                v.setUsername(username);
                v.setPassword(password);
                return v;
            }).getConnection();
        }
        catch (SQLException e)
        {
            throw new ConnectionException(catalogAlias, e);
        }
    }

    private String getPassword(QuerySession session, String catalogAlias)
    {
        Object obj = session.getCatalogProperty(catalogAlias, JdbcCatalog.PASSWORD);
        if (obj instanceof String)
        {
            return (String) obj;
        }
        else if (obj instanceof char[])
        {
            return new String((char[]) obj);
        }
        return null;
    }
}
