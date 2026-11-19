package se.kuseman.payloadbuilder.catalog.mongodb;

import java.util.List;

import org.bson.Document;

import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;

/** A {@link TupleVector} over a batch of Mongo documents. Column vectors are created lazily, only for columns that are actually read. */
class MongoTupleVector implements TupleVector
{
    private final Schema schema;
    private final List<Document> documents;
    private final ValueVector[] columns;

    MongoTupleVector(Schema schema, List<Document> documents)
    {
        this.schema = schema;
        this.documents = documents;
        this.columns = new ValueVector[schema.getSize()];
    }

    @Override
    public Schema getSchema()
    {
        return schema;
    }

    @Override
    public int getRowCount()
    {
        return documents.size();
    }

    @Override
    public ValueVector getColumn(int column)
    {
        ValueVector vector = columns[column];
        if (vector == null)
        {
            vector = new MongoDocumentValueVector(documents, schema.getColumns()
                    .get(column)
                    .getName());
            columns[column] = vector;
        }
        return vector;
    }
}
