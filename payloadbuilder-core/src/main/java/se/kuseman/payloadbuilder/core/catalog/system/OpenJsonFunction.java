package se.kuseman.payloadbuilder.core.catalog.system;

import static java.util.stream.Collectors.toList;

import java.io.Closeable;
import java.io.IOException;
import java.io.InputStream;
import java.io.Reader;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.tuple.Pair;

import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.core.JsonPointer;
import com.fasterxml.jackson.core.JsonToken;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;

/** TVF for opening a json argument and transform into a {@link TupleIterator} */
class OpenJsonFunction extends TableFunctionInfo
{
    static final QualifiedName JSONPATH = QualifiedName.of("jsonpath");

    public OpenJsonFunction()
    {
        super("openjson");
    }

    @Override
    public Arity arity()
    {
        return Arity.ONE;
    }

    private Pair<Closeable, JsonParser> getParser(ValueVector value) throws IOException
    {
        if (value.type()
                .getType() == Type.Any)
        {
            // See if we have a wrapped reader/inputstram. Some catalogs can wrap a lazy reader for a file etc.
            Object obj = value.getAny(0);
            if (obj instanceof Reader r)
            {
                return Pair.of(r, IsJsonFunction.MAPPER.createParser(r));
            }
            else if (obj instanceof InputStream is)
            {
                // Jackson determines encoding based on BOM
                return Pair.of(is, IsJsonFunction.MAPPER.createParser(is));
            }
        }

        return Pair.of(null, IsJsonFunction.MAPPER.createParser(value.valueAsString(0)));
    }

    @Override
    public TupleIterator execute(IExecutionContext context, String catalogAlias, List<IExpression> arguments, FunctionData data)
    {
        ValueVector value = arguments.get(0)
                .eval(context);

        if (value.isNull(0))
        {
            return TupleIterator.EMPTY;
        }

        ValueVector option = context.getOption(JSONPATH, data.getOptions());
        JsonPointer pathPointer = option != null
                && !option.isNull(0)
                        ? JsonPointer.compile(option.getString(0)
                                .toString())
                        : null;

        int batchSize = context.getBatchSize(data.getOptions());

        try
        {
            Pair<Closeable, JsonParser> pair = getParser(value);
            Closeable closable = pair.getKey();
            JsonParser parser = pair.getValue();
            return iterate(context, closable, parser, batchSize, pathPointer);
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error creating a JSON parser", e);
        }
    }

    private TupleIterator iterate(IExecutionContext context, Closeable closable, JsonParser parser, int batchSize, JsonPointer pathPointer)
    {
        return new TupleIterator()
        {
            TupleVector next;
            boolean pathFound = pathPointer == null;
            boolean insideArray = false;
            boolean endFound = false;
            volatile boolean abort = false;

            Runnable abortListener = () ->
            {
                abort = true;
            };

            {
                context.getSession()
                        .registerAbortListener(abortListener);
            }

            @Override
            public TupleVector next()
            {
                if (next == null)
                {
                    throw new NoSuchElementException();
                }
                TupleVector next = this.next;
                this.next = null;
                return next;
            }

            @Override
            public void close()
            {
                context.getSession()
                        .unregisterAbortListener(abortListener);
                IOUtils.closeQuietly(closable, parser);
            }

            @Override
            public boolean hasNext()
            {
                if (next != null)
                {
                    return true;
                }
                else if (endFound)
                {
                    return false;
                }

                try
                {
                    return setNext();
                }
                catch (IOException e)
                {
                    throw new IllegalArgumentException("Error reading JSON", e);
                }
            }

            @SuppressWarnings("unchecked")
            private boolean setNext() throws IOException
            {
                // First stream until we find path pointer
                if (!pathFound)
                {
                    while (!pathFound
                            && parser.nextToken() != null)
                    {
                        if (abort)
                        {
                            return false;
                        }

                        // We only support pointers to objects/arrays
                        boolean isNestedStart = parser.isExpectedStartArrayToken()
                                || parser.isExpectedStartObjectToken();
                        if (isNestedStart)
                        {
                            JsonPointer currentPath = JsonPointer.forPath(parser.getParsingContext(), false);
                            pathFound = pathPointer.equals(currentPath);
                            // pathToken = parser.currentToken();

                            // Mark that we are inside an array because this token will be lost otherwise
                            if (pathFound
                                    && parser.isExpectedStartArrayToken())
                            {
                                insideArray = true;
                                // Move the parse into the first array item
                                parser.nextToken();
                            }
                        }
                    }

                    // No path found => we're done
                    if (!pathFound)
                    {
                        return false;
                    }
                }

                //@formatter:off
                /*
                 * Object Input:
                 * {
                 *   "key": 123,
                 *   "key2": 456
                 * }
                 *
                 * Result
                 * key   key2
                 * 123   456
                 *
                 * Array object input:
                 * [
                 * {
                 *   "key": 123,
                 *   "key2": 456
                 * },
                 * {
                 *   "key": 1230,
                 *   "key2": 4560
                 * }
                 * ]
                 *
                 * Result
                 * key   key2
                 * 123   456
                 * 1230  4560
                 *
                 * Array scalar input
                 * [
                 *   1,
                 *   2,
                 *   [],
                 *   {}
                 * ]
                 *
                 * Result
                 *
                 * value
                 * 1,
                 * 2,
                 * [],
                 * {}
                 *
                 */
                //@formatter:on

                // Start the parser
                if (parser.currentToken() == null)
                {
                    parser.nextToken();
                    if (parser.currentToken() == null)
                    {
                        endFound = true;
                        return false;
                    }
                }

                boolean allMaps = true;
                List<Object> batch = null;

                do
                {
                    if (abort)
                    {
                        return false;
                    }

                    JsonToken currentToken = parser.currentToken();

                    // If we have an end object then we're done
                    // happens if we have a pointer in the middle of a json string
                    // and we have read one object and then end up with the parents end object
                    if (currentToken == JsonToken.END_OBJECT)
                    {
                        endFound = true;
                        break;
                    }

                    if (!insideArray
                            && currentToken == JsonToken.START_ARRAY)
                    {
                        insideArray = true;
                        continue;
                    }
                    // We are done, we reached the end of the top array
                    else if (insideArray
                            && currentToken == JsonToken.END_ARRAY)
                    {
                        endFound = true;
                        break;
                    }

                    allMaps = allMaps
                            && currentToken == JsonToken.START_OBJECT;
                    // Lazy creation of batch
                    if (batch == null)
                    {
                        batch = new ArrayList<>(batchSize);
                    }
                    batch.add(parser.readValueAs(Object.class));

                    if (batch.size() >= batchSize)
                    {
                        break;
                    }
                } while (parser.nextToken() != null);

                if (batch == null)
                {
                    endFound = true;
                    return false;
                }

                final boolean maps;
                final List<String> columns;
                if (allMaps)
                {
                    columns = batch.stream()
                            .map(m -> (Map<String, Object>) m)
                            .flatMap(m -> m.keySet()
                                    .stream())
                            .distinct()
                            .collect(toList());
                    maps = true;
                }
                else
                {
                    columns = List.of("Value");
                    maps = false;
                }

                final List<Object> theBatch = batch;
                final Schema schema = new Schema(columns.stream()
                        .map(c -> new Column(c, ResolvedType.of(Type.Any)))
                        .toList());

                next = new ObjectTupleVector(schema, batch.size(), (row, col) ->
                {
                    Object rowValue = theBatch.get(row);

                    if (maps)
                    {
                        String columnName = columns.get(col);
                        return ((Map<String, Object>) rowValue).get(columnName);
                    }

                    return rowValue;
                });

                return true;
            }
        };
    }
}
