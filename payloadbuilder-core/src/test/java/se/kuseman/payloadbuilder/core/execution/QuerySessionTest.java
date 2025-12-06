package se.kuseman.payloadbuilder.core.execution;

import static java.util.Collections.emptyList;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertSame;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import org.junit.jupiter.api.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.core.QueryException;
import se.kuseman.payloadbuilder.core.catalog.CatalogRegistry;
import se.kuseman.payloadbuilder.core.catalog.CoreColumn;
import se.kuseman.payloadbuilder.core.logicalplan.ALogicalPlanTest;

/** Test of {@link QuerySession}. */
class QuerySessionTest extends ALogicalPlanTest
{
    private QuerySession session = new QuerySession(new CatalogRegistry());

    @Test
    void test_temporary_tables_verify_that_core_columns_are_erased()
    {
        QualifiedName table = QualifiedName.of("table");
        TableSchema tableSchema = new TableSchema(Schema.of(col("col1", Type.Int)));
        assertTrue(tableSchema.getSchema()
                .getColumns()
                .get(0) instanceof CoreColumn);
        session.setTemporaryTableSchema(table, tableSchema);

        TableSchema actual = session.getTemporaryTableSchema(table);
        assertEquals(actual.getSchema(), Schema.of(Column.of("col1", Type.Int)));
        assertFalse(actual.getSchema()
                .getColumns()
                .get(0) instanceof CoreColumn);
    }

    @Test
    void test_temporary_tables()
    {
        QualifiedName table = QualifiedName.of("table");

        assertEquals(emptyList(), session.getTemporaryTables());

        session.dropTemporaryTable(table, true);
        assertThrows(QueryException.class, () -> session.dropTemporaryTable(table, false));

        assertThrows(QueryException.class, () -> session.getTemporaryTableSchema(table));
        assertThrows(QueryException.class, () -> session.getTemporaryTable(table));

        session.setTemporaryTableSchema(table, TableSchema.EMPTY);
        assertEquals(TableSchema.EMPTY, session.getTemporaryTableSchema(table));
        // This should not fail, we overwrite the schema
        TableSchema tableSchema = new TableSchema();
        session.setTemporaryTableSchema(table, tableSchema);
        assertEquals(tableSchema, session.getTemporaryTableSchema(table));

        assertEquals(TableSchema.EMPTY, session.getTemporaryTableSchema(table));

        // Verify that it's ok to set a concrete temporary table into session that already has a table schema
        session.setTemporaryTable(table, new TemporaryTable(TupleVector.EMPTY, emptyList()), false);

        // Start with new session to try with a clean one
        session = new QuerySession(new CatalogRegistry());
        session.setTemporaryTable(table, new TemporaryTable(TupleVector.EMPTY, emptyList()), false);

        assertThrows(QueryException.class, () -> session.setTemporaryTable(table, new TemporaryTable(TupleVector.EMPTY, emptyList()), false));
        // Lenient set
        TemporaryTable tempTable = new TemporaryTable(TupleVector.EMPTY, emptyList());
        session.setTemporaryTable(table, tempTable, true);
        assertSame(tempTable, session.getTemporaryTable(table));

        session.dropTemporaryTable(table, false);

        assertThrows(QueryException.class, () -> session.getTemporaryTableSchema(table));
        assertThrows(QueryException.class, () -> session.getTemporaryTable(table));
    }
}
