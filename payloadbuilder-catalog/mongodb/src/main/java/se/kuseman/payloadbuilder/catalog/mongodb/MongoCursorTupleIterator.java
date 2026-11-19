package se.kuseman.payloadbuilder.catalog.mongodb;

import java.util.ArrayList;
import java.util.Collection;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.NoSuchElementException;
import java.util.stream.Collectors;

import org.bson.Document;

import com.mongodb.client.MongoCursor;

import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.ResolvedType;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;

/**
 * Streams a {@link MongoCursor} in batches of {@link Document}s. If no fixed column set is provided the discovered column set is grown across batches (mirrors the Elasticsearch catalog's dynamic
 * mapping handling) - so earlier batches may report fewer columns than later ones once new fields are encountered.
 */
class MongoCursorTupleIterator implements TupleIterator
{
    private final MongoCursor<Document> cursor;
    private final int batchSize;
    private final List<String> fixedColumns;
    /** Optional describe/analyze stats to update as documents are pulled. Null for callers (eg. table functions) that don't track them. */
    private final MongoDatasource.Data data;
    private LinkedHashSet<String> discoveredColumns;
    private Schema currentSchema;

    MongoCursorTupleIterator(MongoCursor<Document> cursor, int batchSize, List<String> fixedColumns, MongoDatasource.Data data)
    {
        this.cursor = cursor;
        this.batchSize = batchSize;
        this.fixedColumns = fixedColumns;
        this.data = data;
        if (fixedColumns != null)
        {
            this.currentSchema = schemaOf(fixedColumns);
        }
    }

    @Override
    public boolean hasNext()
    {
        return cursor.hasNext();
    }

    @Override
    public TupleVector next()
    {
        if (!cursor.hasNext())
        {
            throw new NoSuchElementException();
        }

        List<Document> batch = new ArrayList<>(batchSize);
        while (cursor.hasNext()
                && batch.size() < batchSize)
        {
            batch.add(cursor.next());
        }

        if (data != null)
        {
            data.documentCount += batch.size();
        }

        if (fixedColumns == null)
        {
            boolean changed = discoveredColumns == null;
            if (discoveredColumns == null)
            {
                discoveredColumns = new LinkedHashSet<>();
            }
            for (Document doc : batch)
            {
                if (discoveredColumns.addAll(doc.keySet()))
                {
                    changed = true;
                }
            }
            if (changed)
            {
                currentSchema = schemaOf(discoveredColumns);
            }
        }

        return new MongoTupleVector(currentSchema, batch);
    }

    @Override
    public void close()
    {
        cursor.close();
        if (data != null)
        {
            data.requestTime.suspend();
        }
    }

    private static Schema schemaOf(Collection<String> columns)
    {
        return new Schema(columns.stream()
                .map(c -> Column.of(c, ResolvedType.ANY))
                .collect(Collectors.toList()));
    }
}
