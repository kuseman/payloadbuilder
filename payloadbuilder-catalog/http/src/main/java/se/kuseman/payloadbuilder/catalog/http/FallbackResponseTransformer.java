package se.kuseman.payloadbuilder.catalog.http;

import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.List;

import org.apache.commons.io.IOUtils;
import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.io.entity.EntityUtils;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.ObjectTupleVector;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.UTF8String;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** Fallback transformer that simply returns the response as a string in a single column */
class FallbackResponseTransformer implements IResponseTransformer
{
    static final Schema HEADERS_SCHEMA = Schema.of(new Column("name", ResolvedType.of(Type.String)), new Column("value", ResolvedType.of(Type.String)));
    static final Schema SCHEMA = new Schema(List.of(new Column("body", ResolvedType.of(Column.Type.String)), new Column("headers", ResolvedType.table(HEADERS_SCHEMA))));

    @Override
    public boolean canHandle(HttpUriRequestBase request, ClassicHttpResponse response)
    {
        return true;
    }

    @Override
    public TupleIterator transform(HttpUriRequestBase request, ClassicHttpResponse response, IExecutionContext context, List<Option> options)
    {
        try
        {
            String body = IOUtils.toString(response.getEntity()
                    .getContent(), StandardCharsets.UTF_8);
            return TupleIterator.singleton(new TupleVector()
            {
                @Override
                public Schema getSchema()
                {
                    return SCHEMA;
                }

                @Override
                public int getRowCount()
                {
                    return 1;
                }

                @Override
                public ValueVector getColumn(int column)
                {
                    return new ValueVector()
                    {
                        @Override
                        public ResolvedType type()
                        {
                            return SCHEMA.getColumns()
                                    .get(column)
                                    .getType();
                        }

                        @Override
                        public int size()
                        {
                            return 1;
                        }

                        @Override
                        public boolean isNull(int row)
                        {
                            if (column == 0)
                            {
                                return body == null;
                            }

                            return false;
                        }

                        @Override
                        public UTF8String getString(int row)
                        {
                            return UTF8String.from(body);
                        }

                        @Override
                        public TupleVector getTable(int row)
                        {
                            final Header[] headers = response.getHeaders();

                            return new ObjectTupleVector(HEADERS_SCHEMA, headers.length, (hrow, hcol) ->
                            {
                                Header header = headers[hrow];
                                return hcol == 0 ? header.getName()
                                        : header.getValue();
                            });
                        }
                    };
                }
            });
        }
        catch (IOException e)
        {
            throw new RuntimeException("Error extracting response body", e);
        }
        finally
        {
            EntityUtils.consumeQuietly(response.getEntity());
        }
    }

}
