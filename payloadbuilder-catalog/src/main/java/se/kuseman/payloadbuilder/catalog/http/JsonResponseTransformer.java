package se.kuseman.payloadbuilder.catalog.http;

import static java.util.Collections.singletonList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.IDatasourceOptions;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Transformer that transforms JSON responses */
public class JsonResponseTransformer implements IResponseTransformer
{
    static final QualifiedName RESPONSE_PATH = QualifiedName.of("jsonresponsepath");
    private static final ObjectMapper MAPPER = new ObjectMapper();

    @Override
    public boolean canHandle(HttpUriRequestBase request, ClassicHttpResponse response)
    {
        // First prio => check the response content type
        Header header = response.getFirstHeader(HttpHeaders.CONTENT_TYPE);
        if (header != null
                && ContentType.APPLICATION_JSON.isSameMimeType(ContentType.parseLenient(header.getValue())))
        {
            return true;
        }

        // Second prio => See if the request contains an Accept for JSON
        header = request.getFirstHeader(HttpHeaders.ACCEPT);
        if (header != null
                && ContentType.APPLICATION_JSON.isSameMimeType(ContentType.parseLenient(header.getValue())))
        {
            return true;
        }

        // This is probably not a JSON response
        return false;
    }

    @Override
    public TupleIterator transform(HttpUriRequestBase request, ClassicHttpResponse response, IExecutionContext context, IDatasourceOptions options)
    {
        ValueVector vv = options.getOption(RESPONSE_PATH, context);
        String jsonresponsepath = vv == null ? null
                : vv.getString(0)
                        .toString();

        try
        {
            if (jsonresponsepath != null)
            {
                JsonNode node = MAPPER.readTree(response.getEntity()
                        .getContent());
                if (node == null
                        || node.isNull())
                {
                    return TupleIterator.EMPTY;
                }
                JsonNode at = node.at(jsonresponsepath);
                if (at.isMissingNode())
                {
                    return TupleIterator.EMPTY;
                }
                return TupleIterator.singleton(convertToTupleVector(MAPPER.treeToValue(at, Object.class)));
            }

            Object value = MAPPER.readValue(response.getEntity()
                    .getContent(), Object.class);
            if (value == null)
            {
                return TupleIterator.EMPTY;
            }
            return TupleIterator.singleton(convertToTupleVector(value));
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error transforming JSON into a TupleIterator", e);
        }

    }

    /** Convert response value to a {@link TupleVector} */
    @SuppressWarnings("unchecked")
    private static TupleVector convertToTupleVector(final Object value)
    {
        requireNonNull(value);
        List<Map<String, Object>> maps;

        if (value instanceof Map)
        {
            maps = singletonList((Map<String, Object>) value);
        }
        else if (value instanceof List)
        {
            maps = (List<Map<String, Object>>) value;
        }
        else
        {
            throw new IllegalAccessError("Cannot convert value " + value + " to a TupleVector");
        }

        List<String> columns = maps.stream()
                .flatMap(m -> m.keySet()
                        .stream())
                .distinct()
                .collect(toList());

        final Schema schema = new Schema(columns.stream()
                .map(c -> new Column(c, ResolvedType.of(Type.Any)))
                .toList());

        return new ObjectTupleVector(schema, maps.size(), (row, column) ->
        {
            String columnName = columns.get(column);
            return maps.get(row)
                    .get(columnName);
        });
    }
}
