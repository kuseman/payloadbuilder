package se.kuseman.payloadbuilder.catalog.http;

import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.io.InputStream;
import java.util.List;
import java.util.Optional;

import org.apache.hc.client5.http.classic.methods.HttpUriRequestBase;
import org.apache.hc.core5.http.ClassicHttpResponse;
import org.apache.hc.core5.http.ContentType;
import org.apache.hc.core5.http.Header;
import org.apache.hc.core5.http.HttpHeaders;

import se.kuseman.payloadbuilder.api.catalog.Catalog;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.TableFunctionInfo;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;

/** Base transformer that forwards the response to a system catalog TVF */
public abstract class ATableFunctionForwardResponseTransformer implements IResponseTransformer
{
    private final String function;
    private final ContentType contentType;

    public ATableFunctionForwardResponseTransformer(String function, ContentType contentType)
    {
        this.function = requireNonNull(function);
        this.contentType = requireNonNull(contentType);
    }

    @Override
    public boolean canHandle(HttpUriRequestBase request, ClassicHttpResponse response)
    {
        // First prio => check the response content type
        Header header = response.getFirstHeader(HttpHeaders.CONTENT_TYPE);
        if (header != null
                && contentType.isSameMimeType(ContentType.parseLenient(header.getValue())))
        {
            return true;
        }

        // Second prio => See if the request contains an Accept for our content type
        header = request.getFirstHeader(HttpHeaders.ACCEPT);
        if (header != null
                && contentType.isSameMimeType(ContentType.parseLenient(header.getValue())))
        {
            return true;
        }

        // This is probably not a response we can handle
        return false;
    }

    @Override
    public TupleIterator transform(HttpUriRequestBase request, ClassicHttpResponse response, IExecutionContext context, List<Option> options) throws IOException
    {
        // Redirect this response to core function
        TableFunctionInfo openJsonFunction = context.getSession()
                .getSystemCatalog()
                .getTableFunction(function);

        final InputStream is = response.getEntity()
                .getContent();
        IExpression arg = new IExpression()
        {
            @Override
            public ResolvedType getType()
            {
                return ResolvedType.of(Type.Any);
            }

            @Override
            public ValueVector eval(TupleVector input, IExecutionContext context)
            {
                return ValueVector.literalAny(is);
            }

            @Override
            public <T, C> T accept(IExpressionVisitor<T, C> visitor, C context)
            {
                throw new RuntimeException("Cannot be visited");
            }
        };

        return openJsonFunction.execute(context, Catalog.SYSTEM_CATALOG_ALIAS, Optional.empty(), List.of(arg), options);
    }
}
