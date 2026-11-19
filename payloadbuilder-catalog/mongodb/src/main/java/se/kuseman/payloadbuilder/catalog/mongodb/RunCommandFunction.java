package se.kuseman.payloadbuilder.catalog.mongodb;

import java.util.List;
import java.util.stream.Collectors;

import org.bson.Document;

import com.mongodb.client.MongoClient;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/**
 * TVF that runs a raw MongoDB server command against a database - the driver equivalent of mongosh's {@code db.runCommand(...)}.
 *
 * <pre>
 * NOTE! Complements {@link AggregateFunction}: aggregate() only reaches the aggregation pipeline language (a subset of server capability), while
 * most maintenance/administrative operations (dbStats, serverStatus, currentOp, createIndexes, dropIndexes, collMod, compact, renameCollection, ...)
 * are server commands, not pipeline stages, and are only reachable through runCommand(). Deliberately unrestricted, same posture as aggregate() -
 * what it's actually allowed to do server side is gated by the connected user's Mongo role, same as any other Mongo client.
 * </pre>
 */
class RunCommandFunction extends TableFunctionInfo
{
    private final MongoCatalog catalog;

    RunCommandFunction(MongoCatalog catalog)
    {
        super("runCommand");
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
        return "Table valued function that runs a raw MongoDB server command against a database - the driver equivalent of mongosh's db.runCommand(...)." + System.lineSeparator()
               + "runCommand(<database expression>, <command JSON document expression>)"
               + System.lineSeparator()
               + "ex. select * from mongo#runCommand('mydb', '{\"dbStats\": 1}')"
               + System.lineSeparator()
               + "ex. select * from mongo#runCommand('admin', '{\"currentOp\": 1}')";
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData data)
    {
        if (arguments.size() != 2)
        {
            throw new IllegalArgumentException("runCommand function takes 2 arguments. 'runCommand(<database>, <command JSON>)'");
        }

        String database = String.valueOf(arguments.get(0)
                .eval(context)
                .valueAsObject(0));
        String commandJson = String.valueOf(arguments.get(1)
                .eval(context)
                .valueAsObject(0));
        // Validated up front, before acquiring a client, so malformed input fails fast regardless of connection state
        Document command = parseCommand(commandJson);

        MongoClient client = catalog.getClient(context.getSession(), catalogAlias);
        Document result = client.getDatabase(database)
                .runCommand(command);

        Schema schema = new Schema(result.keySet()
                .stream()
                .map(k -> Column.of(k, ResolvedType.ANY))
                .collect(Collectors.toList()));

        return TupleIterator.singleton(new MongoTupleVector(schema, List.of(result)));
    }

    private static Document parseCommand(String commandJson)
    {
        try
        {
            return Document.parse(commandJson);
        }
        catch (Exception e)
        {
            throw new IllegalArgumentException("runCommand() command is not a valid JSON object: " + e.getMessage(), e);
        }
    }
}
