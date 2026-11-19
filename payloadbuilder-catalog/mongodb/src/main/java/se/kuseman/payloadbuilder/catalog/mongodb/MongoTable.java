package se.kuseman.payloadbuilder.catalog.mongodb;

import se.kuseman.payloadbuilder.api.QualifiedName;

/** Resolves a table's qualified name into a MongoDB database/collection pair. Tables are addressed as {@code <database>.<collection>}. */
record MongoTable(String database, String collection)
{
    static MongoTable of(QualifiedName table)
    {
        if (table.size() != 2)
        {
            throw new IllegalArgumentException("Expected a two part table name '<database>.<collection>' but got: " + table);
        }
        return new MongoTable(table.getParts()
                .get(0), table.getLast());
    }
}
