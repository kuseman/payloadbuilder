package se.kuseman.payloadbuilder.catalog.http;

import static java.util.Collections.emptyList;
import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang3.StringUtils;
import org.apache.hc.client5.http.config.ConnectionConfig;
import org.apache.hc.client5.http.impl.classic.CloseableHttpClient;
import org.apache.hc.client5.http.impl.classic.HttpClients;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManager;
import org.apache.hc.client5.http.impl.io.PoolingHttpClientConnectionManagerBuilder;
import org.apache.hc.core5.http.HttpHeaders;
import org.apache.hc.core5.util.TimeValue;
import org.apache.hc.core5.util.Timeout;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.CompileException;
import se.kuseman.payloadbuilder.api.catalog.DatasourceData;
import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Index.ColumnsType;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/**
 * Catalog that supports HTTP requests
 * 
 * <pre>
 * Works as a table that supports indices.
 * 
 * Endpoint is specified as the table source, if table source starts with http
 * then the tablesource itself is considered to be the endpoint else the endpoint
 * is taken from a catalog property with the same name. This to enable dynamic
 * endpoints
 * 
 * Ex. 
 * 
 * from http#"http://path.to.endpoint" x
 * 
 * or
 * 
 * use http.endpoint = 'http://path.to.endpoint'
 * from http#endpoint x
 * 
 * 
 * A set of properties can be specified as table options to customize the request.
 * Different place holders can be added in various pattern properties.
 * - {{field}}     => This will enable 'field' as an indexed column which is intended to be used as pushdown predicates / index access
 * 
 * NOTE! A gotcha here is that the "field" MUST be present in the resulting payload else all predicates (where/join) will result in no hits.
 *
 * from http#"http://path/to/endpoint/v1" x with (
 *   method = 'post',                                       // Method of HTTP request
 *   header."Content-Type" = 'application/json',            // Headers to add. All qualifiers that has the first part = 'header' will be added
 *   queryPattern = '/{{id}}?color={{color}}',              // Query pattern that contains place holders for index fields. Names between brackets will be eligable as index fields on table source
 *   bodyPattern = '
 *   {
 *     "filter": {
 *       "sizes": [{{size}}]
 *     }
 *   }'
 * )
 * 
 * If there are any index fields in either query or body all those must be present as either
 * seek predicate or push down predicate fields else a data source cannot be created
 * 
 * </pre>
 */
public class HttpCatalog extends Catalog
{
    public static final String NAME = "HttpCatalog";
    static final String HEADER = "header";
    static final QualifiedName METHOD = QualifiedName.of("method");
    static final String QUERY_PATTERN = "querypattern";
    static final String BODY_PATTERN = "bodypattern";
    static final QualifiedName FAIL_ON_NON_200 = QualifiedName.of("failonnon200");
    static final QualifiedName CONNECT_TIMEOUT = QualifiedName.of("connecttimeout");
    static final QualifiedName RECEIVE_TIMEOUT = QualifiedName.of("receivetimeout");
    static final int DEFAULT_RECIEVE_TIMEOUT = 15000;
    static final int DEFAULT_CONNECT_TIMEOUT = 1500;

    /** Placeholder regex */
    static final Pattern PLACEHOLDER_PATTERN = Pattern.compile("\\{\\{(.+?)\\}\\}");

    private final List<IResponseTransformer> responseTransformers;
    private final CloseableHttpClient httpClient;

    public HttpCatalog()
    {
        this(List.of(new JsonResponseTransformer(), new CsvResponseTransformer(), new XmlResponseTransformer()));
    }

    public HttpCatalog(List<IResponseTransformer> responseTransformers)
    {
        super(NAME);
        this.responseTransformers = requireNonNull(responseTransformers, "responseTransformers");

        PoolingHttpClientConnectionManager manager = PoolingHttpClientConnectionManagerBuilder.create()
                .setDefaultConnectionConfig(ConnectionConfig.custom()
                        .setConnectTimeout(Timeout.ofMilliseconds(DEFAULT_CONNECT_TIMEOUT))
                        .setSocketTimeout(Timeout.ofMilliseconds(DEFAULT_RECIEVE_TIMEOUT))
                        .build())
                .build();
        this.httpClient = HttpClients.custom()
                .setConnectionManager(manager)
                .evictExpiredConnections()
                .evictIdleConnections(TimeValue.ofSeconds(60))
                .build();

        registerFunction(new QueryFunction(httpClient, responseTransformers));
    }

    @Override
    public void close()
    {
        try
        {
            httpClient.close();
        }
        catch (IOException e)
        {
        }
    }

    @Override
    public TableSchema getTableSchema(IQuerySession session, String catalogAlias, QualifiedName table, List<Option> options)
    {
        // Collect indices in options
        Map<String, String> fields = new HashMap<>();
        for (Option option : options)
        {
            String firstPart = option.getOption()
                    .getFirst();
            if (option.getOption()
                    .size() == 1
                    && (QUERY_PATTERN.equalsIgnoreCase(firstPart)
                            || BODY_PATTERN.equalsIgnoreCase(firstPart)))
            {
                // Inspect value expression and find field place holders
                // these will be our index columns
                addIndexFields(fields, option.getValueExpression());
            }
        }

        return new TableSchema(Schema.EMPTY, !fields.isEmpty() ? singletonList(new Index(table, new ArrayList<>(fields.values()), ColumnsType.ALL))
                : emptyList());
    }

    @Override
    public IDatasource getScanDataSource(IQuerySession session, String catalogAlias, QualifiedName table, DatasourceData data)
    {
        return getDataSource(session, catalogAlias, table, null, data);
    }

    @Override
    public IDatasource getSeekDataSource(IQuerySession session, String catalogAlias, ISeekPredicate seekPredicate, DatasourceData data)
    {
        return getDataSource(session, catalogAlias, seekPredicate.getIndex()
                .getTable(), seekPredicate, data);
    }

    private IDatasource getDataSource(IQuerySession session, String catalogAlias, QualifiedName table, ISeekPredicate seekPredicate, DatasourceData data)
    {
        if (table.size() != 1)
        {
            throw new CompileException("Tables qualifiers for " + NAME + " only supportes one part.");
        }

        Map<String, String> fields = new HashMap<>();
        Request request = getRequest(session, data.getOptions(), data.getPredicates(), fields);
        String endpoint = table.getFirst();
        // Validate that we are indexing prefixed columns
        // We can only make index request against GET/POST ones
        if (seekPredicate != null)
        {
            // Remove the index columns from the placeholder fields
            // This to determine if a data source can be created or not.
            // If there are placeholders all of those must be satisfied either through index
            // or push down predicates
            for (String indexColumn : seekPredicate.getIndexColumns())
            {
                fields.remove(StringUtils.lowerCase(indexColumn));
            }
        }

        if (!fields.isEmpty())
        {
            throw new CompileException("All request placeholders must be used. Placeholders not processed: " + fields.values());
        }

        return new HttpDataSource(httpClient, catalogAlias, endpoint, seekPredicate, request, responseTransformers);
    }

    private void addIndexFields(Map<String, String> fields, IExpression expression)
    {
        String value = expression.toString();
        Matcher matcher = PLACEHOLDER_PATTERN.matcher(value);
        while (matcher.find())
        {
            String group = matcher.group(1);
            fields.put(group.toLowerCase(), group);
        }
    }

    private Request getRequest(IQuerySession session, List<Option> options, List<IPredicate> predicates, Map<String, String> fields)
    {
        IExpression queryExpression = null;
        IExpression bodyExpression = null;
        IExpression contentType = null;

        for (Option option : options)
        {
            QualifiedName qname = option.getOption();

            if (qname.size() == 2
                    && HEADER.equalsIgnoreCase(option.getOption()
                            .getFirst()))
            {
                String headerName = option.getOption()
                        .getParts()
                        .get(1);

                if (HttpHeaders.CONTENT_TYPE.equalsIgnoreCase(headerName))
                {
                    contentType = option.getValueExpression();
                }
            }
            else if (qname.size() == 1
                    && QUERY_PATTERN.equalsIgnoreCase(qname.getFirst()))
            {
                queryExpression = option.getValueExpression();
                addIndexFields(fields, option.getValueExpression());
            }
            else if (qname.size() == 1
                    && BODY_PATTERN.equalsIgnoreCase(qname.getFirst()))
            {
                bodyExpression = option.getValueExpression();
                addIndexFields(fields, option.getValueExpression());
            }
        }

        // Collect push down predicates
        List<Predicate> requestPredicates = new ArrayList<>();
        Iterator<IPredicate> iterator = predicates.iterator();
        while (iterator.hasNext())
        {
            IPredicate predicate = iterator.next();
            Predicate requestPredicate = getPredicate(predicate, fields);
            if (requestPredicate != null)
            {
                requestPredicates.add(requestPredicate);
                iterator.remove();
            }
        }

        return new Request(contentType, queryExpression, bodyExpression, requestPredicates);
    }

    private Predicate getPredicate(IPredicate predicate, Map<String, String> singleValueIndices)
    {
        QualifiedName column = predicate.getQualifiedColumn();
        if (column == null
                || column.size() != 1)
        {
            return null;
        }

        String lowerColumn = column.getFirst()
                .toLowerCase();
        String indexColumn = singleValueIndices.get(lowerColumn);
        if (indexColumn == null)
        {
            return null;
        }

        if (predicate.getType() == IPredicate.Type.COMPARISION
                && predicate.getComparisonType() == IComparisonExpression.Type.EQUAL)
        {
            singleValueIndices.remove(lowerColumn);
            return new Predicate(indexColumn, singletonList(predicate.getComparisonExpression()));
        }
        else if (predicate.getType() == IPredicate.Type.IN
                && !predicate.getInExpression()
                        .isNot())
        {
            singleValueIndices.remove(lowerColumn);
            return new Predicate(indexColumn, predicate.getInExpression()
                    .getArguments());
        }
        return null;
    }

    record Request(IExpression contentType, IExpression queryExpression, IExpression bodyExpression, List<Predicate> predicates)
    {
    }

    record Predicate(String name, List<IExpression> values)
    {
    }
}
