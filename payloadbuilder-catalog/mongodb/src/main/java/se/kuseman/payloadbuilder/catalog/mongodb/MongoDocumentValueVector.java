package se.kuseman.payloadbuilder.catalog.mongodb;

import java.util.List;

import org.bson.Document;

import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/**
 * A {@link ValueVector} over a single column of a batch of Mongo documents. Converts the underlying BSON value to a PLB value lazily on first access and caches the result, so predicates/sorts that
 * only touch a few metadata fields never pay for converting large/expensive columns that are never read.
 */
class MongoDocumentValueVector implements ValueVector
{
    private final List<Document> documents;
    private final String field;
    private final Object[] cache;
    private final boolean[] converted;

    MongoDocumentValueVector(List<Document> documents, String field)
    {
        this.documents = documents;
        this.field = field;
        this.cache = new Object[documents.size()];
        this.converted = new boolean[documents.size()];
    }

    @Override
    public ResolvedType type()
    {
        return ResolvedType.ANY;
    }

    @Override
    public int size()
    {
        return documents.size();
    }

    @Override
    public boolean isNull(int row)
    {
        return getAny(row) == null;
    }

    @Override
    public Object getAny(int row)
    {
        if (!converted[row])
        {
            cache[row] = BsonValueConverter.fromBson(documents.get(row)
                    .get(field));
            converted[row] = true;
        }
        return cache[row];
    }
}
