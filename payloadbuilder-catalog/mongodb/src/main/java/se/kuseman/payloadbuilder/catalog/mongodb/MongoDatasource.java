package se.kuseman.payloadbuilder.catalog.mongodb;

import static java.util.stream.Collectors.joining;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import org.apache.commons.lang3.time.DurationFormatUtils;
import org.apache.commons.lang3.time.StopWatch;
import org.bson.Document;
import org.bson.conversions.Bson;

import com.mongodb.MongoClientSettings;
import com.mongodb.client.FindIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;
import com.mongodb.client.model.Filters;

import se.kuseman.payloadbuilder.api.catalog.IDatasource;
import se.kuseman.payloadbuilder.api.catalog.IPredicate;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate;
import se.kuseman.payloadbuilder.api.execution.ISeekPredicate.ISeekKey;
import se.kuseman.payloadbuilder.api.execution.NodeData;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** {@link IDatasource} for MongoDB. Used for both full scans and index seeks. */
class MongoDatasource implements IDatasource
{
    private final int nodeId;
    private final MongoClientHolder clientHolder;
    private final String catalogAlias;
    private final MongoTable table;
    private final ISeekPredicate seekPredicate;
    /** Predicates consumed from the planner. Value expressions are only evaluated once a live {@link IExecutionContext} is available, in {@link #execute(IExecutionContext)}. */
    private final List<IPredicate> predicates;
    private final Bson sort;
    private final Bson projection;
    /** Fixed set of columns requested via projection pushdown, or null if the column set should be discovered dynamically from returned documents. */
    private final List<String> projectedColumns;
    private final List<Option> options;

    MongoDatasource(int nodeId, MongoClientHolder clientHolder, String catalogAlias, MongoTable table, ISeekPredicate seekPredicate, List<IPredicate> predicates, Bson sort, Bson projection,
            List<String> projectedColumns, List<Option> options)
    {
        this.nodeId = nodeId;
        this.clientHolder = clientHolder;
        this.catalogAlias = catalogAlias;
        this.table = table;
        this.seekPredicate = seekPredicate;
        this.predicates = predicates;
        this.sort = sort;
        this.projection = projection;
        this.projectedColumns = projectedColumns;
        this.options = options;
    }

    @Override
    public TupleIterator execute(IExecutionContext context)
    {
        Bson filter = buildFilter(context);

        Data data = context.getStatementContext()
                .getOrCreateNodeData(nodeId, Data::new);
        data.requestCount++;
        data.actualFilter = toJson(filter);
        data.actualSort = sort != null ? toJson(sort)
                : null;
        data.actualProjection = projection != null ? toJson(projection)
                : null;
        data.requestTime.resume();

        MongoClient client = clientHolder.getClient(context.getSession(), catalogAlias);
        MongoCollection<Document> collection = client.getDatabase(table.database())
                .getCollection(table.collection());
        int batchSize = context.getBatchSize(options);

        FindIterable<Document> findIterable = collection.find(filter)
                .batchSize(batchSize);
        if (sort != null)
        {
            findIterable = findIterable.sort(sort);
        }
        if (projection != null)
        {
            findIterable = findIterable.projection(projection);
        }

        return new MongoCursorTupleIterator(findIterable.iterator(), batchSize, projectedColumns, data);
    }

    @Override
    public Map<String, Object> getDescribeProperties(IExecutionContext context)
    {
        Map<String, Object> result = new LinkedHashMap<>();
        result.put(CATALOG, MongoCatalog.NAME);
        result.put(PREDICATE, predicates.stream()
                .map(IPredicate::getSqlRepresentation)
                .collect(joining(" AND ")));
        if (seekPredicate != null)
        {
            result.put(INDEX, seekPredicate.getIndex()
                    .toString());
        }
        result.put("Sort", sort != null ? toJson(sort)
                : "");
        result.put("Projection", projection != null ? toJson(projection)
                : "*");
        result.put("Query", toJson(buildFilter(context)));

        Data data = context.getStatementContext()
                .getNodeData(nodeId);
        if (data != null)
        {
            result.put("Request count", data.requestCount);
            result.put("Document count", data.documentCount);
            result.put("Request time", DurationFormatUtils.formatDurationHMS(data.requestTime.getTime(TimeUnit.MILLISECONDS)));
            if (!isBlank(data.actualFilter))
            {
                result.put("Query", data.actualFilter);
            }
            if (!isBlank(data.actualSort))
            {
                result.put("Sort", data.actualSort);
            }
            if (!isBlank(data.actualProjection))
            {
                result.put("Projection", data.actualProjection);
            }
        }

        return result;
    }

    private Bson buildFilter(IExecutionContext context)
    {
        List<Bson> all = new ArrayList<>(MongoFilterBuilder.build(context, predicates));
        if (seekPredicate != null)
        {
            all.add(buildSeekFilter(context));
        }

        if (all.isEmpty())
        {
            return new Document();
        }
        return all.size() == 1 ? all.get(0)
                : Filters.and(all);
    }

    private Bson buildSeekFilter(IExecutionContext context)
    {
        String column = seekPredicate.getIndexColumns()
                .get(0);
        List<ISeekKey> seekKeys = seekPredicate.getSeekKeys(context);
        ValueVector keys = seekKeys.get(0)
                .getValue();

        List<Object> values = new ArrayList<>(keys.size());
        for (int i = 0; i < keys.size(); i++)
        {
            if (!keys.isNull(i))
            {
                values.add(BsonValueConverter.toBson(keys.valueAsObject(i), column));
            }
        }
        return Filters.in(column, values);
    }

    private static String toJson(Bson bson)
    {
        return bson.toBsonDocument(Document.class, MongoClientSettings.getDefaultCodecRegistry())
                .toJson();
    }

    /** Node data with Mongo specific stats, shown in DESCRIBE/ANALYZE output. */
    static class Data extends NodeData
    {
        final StopWatch requestTime = new StopWatch();
        int requestCount;
        long documentCount;
        String actualFilter;
        String actualSort;
        String actualProjection;

        Data()
        {
            requestTime.start();
            requestTime.suspend();
        }
    }
}
