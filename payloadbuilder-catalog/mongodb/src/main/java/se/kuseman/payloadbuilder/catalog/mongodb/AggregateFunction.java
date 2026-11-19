package se.kuseman.payloadbuilder.catalog.mongodb;

import java.util.ArrayList;
import java.util.List;

import org.bson.Document;

import com.mongodb.client.AggregateIterable;
import com.mongodb.client.MongoClient;
import com.mongodb.client.MongoCollection;

import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** TVF that runs a raw MongoDB aggregation pipeline against a collection - an escape hatch for queries predicate/sort/projection pushdown cannot express. */
class AggregateFunction extends TableFunctionInfo
{
    private final MongoCatalog catalog;

    AggregateFunction(MongoCatalog catalog)
    {
        super("aggregate");
        this.catalog = catalog;
    }

    @Override
    public Arity arity()
    {
        return Arity.TWO;
    }

    @Override
    public String getDescription()
    {
        return "Table valued function that runs a raw MongoDB aggregation pipeline against a collection." + System.lineSeparator()
               + "aggregate(<'database.collection' expression>, <pipeline JSON array expression>)"
               + System.lineSeparator()
               + "ex. select * from mongo#aggregate('mydb.mycollection', '[{\"$match\": {\"active\": true}}]')";
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData data)
    {
        if (arguments.size() != 2)
        {
            throw new IllegalArgumentException("aggregate function takes 2 arguments. 'aggregate(<database.collection>, <pipeline JSON array>)'");
        }

        String tableName = String.valueOf(arguments.get(0)
                .eval(context)
                .valueAsObject(0));
        String pipelineJson = String.valueOf(arguments.get(1)
                .eval(context)
                .valueAsObject(0));

        String[] parts = tableName.split("\\.", 2);
        if (parts.length != 2)
        {
            throw new IllegalArgumentException("Expected a two part table name '<database>.<collection>' but got: " + tableName);
        }
        MongoTable table = new MongoTable(parts[0], parts[1]);
        // Validated up front, before acquiring a client, so malformed input fails fast regardless of connection state
        List<Document> pipeline = parsePipeline(pipelineJson);

        MongoClient client = catalog.getClient(context.getSession(), catalogAlias);
        MongoCollection<Document> collection = client.getDatabase(table.database())
                .getCollection(table.collection());

        int batchSize = context.getBatchSize(data.getOptions());
        AggregateIterable<Document> aggregateIterable = collection.aggregate(pipeline)
                .batchSize(batchSize);

        return new MongoCursorTupleIterator(aggregateIterable.iterator(), batchSize, null, null);
    }

    /**
     * Parses the pipeline JSON and validates that it's an array of stage <b>objects</b>.
     *
     * <pre>
     * NOTE! Without this validation a malformed stage (eg. a stray nested array instead of a stage document) would pass an unchecked
     * generics cast here silently, and only fail later as a confusing ClassCastException deep inside the driver's BSON encoding once
     * the pipeline is actually sent - with no indication of which stage was wrong. Failing fast here with a precise, 1-based stage
     * index gives a message that's actually actionable.
     * </pre>
     */
    private static List<Document> parsePipeline(String pipelineJson)
    {
        Document wrapper;
        try
        {
            // Wrap the pipeline array in a document so the driver's JSON parser can turn each stage into a Document
            wrapper = Document.parse("{\"pipeline\":" + pipelineJson + "}");
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("aggregate() pipeline is not valid JSON: " + e.getMessage(), e);
        }

        Object rawPipeline = wrapper.get("pipeline");
        if (!(rawPipeline instanceof List))
        {
            throw new IllegalArgumentException("aggregate() pipeline must be a JSON array of stage objects (eg. '[{\"$match\": {...}}]'), got: " + describe(rawPipeline));
        }

        List<?> rawStages = (List<?>) rawPipeline;
        List<Document> pipeline = new ArrayList<>(rawStages.size());
        for (int i = 0; i < rawStages.size(); i++)
        {
            Object stage = rawStages.get(i);
            if (!(stage instanceof Document))
            {
                throw new IllegalArgumentException("aggregate() pipeline stage " + (i + 1) + " must be a JSON object (eg. {\"$match\": {...}}), got: " + describe(stage));
            }
            pipeline.add((Document) stage);
        }
        return pipeline;
    }

    private static String describe(Object value)
    {
        if (value == null)
        {
            return "null";
        }
        return value.getClass()
                .getSimpleName()
               + " ("
               + value
               + ")";
    }
}
