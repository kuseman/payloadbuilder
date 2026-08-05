package se.kuseman.payloadbuilder.catalog.mongodb;

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

        MongoClient client = catalog.getClient(context.getSession(), catalogAlias);
        MongoCollection<Document> collection = client.getDatabase(table.database())
                .getCollection(table.collection());

        int batchSize = context.getBatchSize(data.getOptions());
        AggregateIterable<Document> aggregateIterable = collection.aggregate(parsePipeline(pipelineJson))
                .batchSize(batchSize);

        return new MongoCursorTupleIterator(aggregateIterable.iterator(), batchSize, null, null);
    }

    @SuppressWarnings("unchecked")
    private static List<Document> parsePipeline(String pipelineJson)
    {
        // Wrap the pipeline array in a document so the driver's JSON parser can turn each stage into a Document
        Document wrapper = Document.parse("{\"pipeline\":" + pipelineJson + "}");
        return (List<Document>) (List<?>) wrapper.get("pipeline", List.class);
    }
}
