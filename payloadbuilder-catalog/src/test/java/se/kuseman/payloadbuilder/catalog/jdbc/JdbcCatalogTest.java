package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Collections.emptyList;
import static org.junit.Assert.assertEquals;
import static org.mockito.Mockito.mock;

import java.util.List;

import org.junit.Test;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.Column;
import se.kuseman.payloadbuilder.api.catalog.Column.Type;
import se.kuseman.payloadbuilder.api.catalog.Index;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.catalog.TableSchema;
import se.kuseman.payloadbuilder.api.execution.IQuerySession;
import se.kuseman.payloadbuilder.core.expression.LiteralStringExpression;

/** Test of {@link JdbcCatalog}. */
public class JdbcCatalogTest
{
    @Test
    public void test_tableSchema()
    {
        JdbcCatalog c = new JdbcCatalog();

        QualifiedName tableName = QualifiedName.of("schema", "table");

        TableSchema actual = c.getTableSchema(mock(IQuerySession.class), "jdbc", tableName, List.of());
        assertEquals(Schema.EMPTY, actual.getSchema());
        assertEquals(List.of(new Index(tableName, emptyList(), Index.ColumnsType.WILDCARD)), actual.getIndices());

        actual = c.getTableSchema(mock(IQuerySession.class), "jdbc", tableName, List.of(new Option(JdbcCatalog.PROJECTION, new LiteralStringExpression("col1,col2, col3"))));
        assertEquals(Schema.of(Column.of("col1", Type.Any), Column.of("col2", Type.Any), Column.of("col3", Type.Any)), actual.getSchema());
        assertEquals(List.of(new Index(tableName, emptyList(), Index.ColumnsType.WILDCARD)), actual.getIndices());
    }
}
