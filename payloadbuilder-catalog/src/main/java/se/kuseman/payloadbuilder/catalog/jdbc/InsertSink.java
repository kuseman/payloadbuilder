package se.kuseman.payloadbuilder.catalog.jdbc;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.joining;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.catalog.IDatasink;
import se.kuseman.payloadbuilder.api.catalog.Option;
import se.kuseman.payloadbuilder.api.catalog.Schema;
import se.kuseman.payloadbuilder.api.execution.IExecutionContext;
import se.kuseman.payloadbuilder.api.execution.TupleIterator;
import se.kuseman.payloadbuilder.api.execution.TupleVector;
import se.kuseman.payloadbuilder.api.execution.ValueVector;
import se.kuseman.payloadbuilder.catalog.jdbc.JdbcCatalog.ColumnOption;
import se.kuseman.payloadbuilder.catalog.jdbc.dialect.DialectProvider;
import se.kuseman.payloadbuilder.catalog.jdbc.dialect.SqlDialect;

/** Sink for SelectInto/InsertInto */
class InsertSink implements IDatasink
{
    private static final Logger LOGGER = LoggerFactory.getLogger(InsertSink.class);
    private final JdbcCatalog catalog;
    private final QualifiedName table;
    private final String catalogAlias;
    private final List<String> insertIntoColumns;
    private final Map<String, ColumnOption> columnOptions;

    InsertSink(JdbcCatalog catalog, QualifiedName table, String catalogAlias, List<Option> options)
    {
        this(catalog, table, catalogAlias, emptyList(), options);
    }

    InsertSink(JdbcCatalog catalog, QualifiedName table, String catalogAlias, List<String> insertIntoColumns, List<Option> options)
    {
        this.catalog = catalog;
        this.table = table;
        this.catalogAlias = catalogAlias;
        this.insertIntoColumns = requireNonNull(insertIntoColumns);
        this.columnOptions = ColumnOption.extract(options);
    }

    @Override
    public void execute(IExecutionContext context, TupleIterator input)
    {
        String database = catalog.getDatabase(context.getSession(), catalogAlias);
        SqlDialect dialect = DialectProvider.getDialect(context.getSession(), catalogAlias);
        AtomicReference<Statement> currentStatemnet = new AtomicReference<>();

        Runnable abortListener = () ->
        {
            Statement statement = currentStatemnet.get();
            JdbcUtils.cancelQuiet(statement);
        };
        context.getSession()
                .registerAbortListener(abortListener);

        try (Connection connection = catalog.getInsertConnection(context.getSession(), catalogAlias))
        {
            if (dialect.usesSchemaAsDatabase())
            {
                connection.setSchema(database);
            }
            else
            {
                connection.setCatalog(database);
            }
            // TODO: transaction settings via options
            // NONE -> setAutoCommit(true)
            // BATCH -> commit between each batch
            // FULL -> commit after whole insert
            connection.setAutoCommit(false);
            Schema schema = null;
            int colCount = -1;
            int totalRowCount = 0;
            int batchCount = 0;
            String preparedStatement = null;
            try
            {
                while (input.hasNext())
                {
                    // CSOFF
                    long time = System.nanoTime();
                    // CSON
                    TupleVector next = input.next();
                    if (schema == null)
                    {
                        schema = next.getSchema();
                        colCount = schema.getSize();
                    }
                    else if (!schema.equals(next.getSchema()))
                    {
                        throw new RuntimeException("JDBC insert expects that all batches have the same schema. Schema used: " + schema + " batch schema: " + next.getSchema());
                    }

                    if (preparedStatement == null)
                    {
                        preparedStatement = createTableAndPreparedStatement(dialect, connection, schema);
                    }

                    ValueVector[] columns = new ValueVector[colCount];
                    int rowCount = next.getRowCount();
                    for (int j = 0; j < colCount; j++)
                    {
                        columns[j] = next.getColumn(j);
                    }
                    try (PreparedStatement stm = connection.prepareStatement(preparedStatement))
                    {
                        currentStatemnet.set(stm);
                        for (int i = 0; i < rowCount; i++)
                        {
                            for (int j = 0; j < colCount; j++)
                            {
                                dialect.setStatementValue(columns[j].type()
                                        .getType(), stm, j + 1, i, columns[j]);
                            }
                            stm.addBatch();
                        }
                        stm.executeBatch();
                        connection.commit();
                    }
                    totalRowCount += next.getRowCount();
                    batchCount++;
                    LOGGER.debug("Inserted batch: {}, total rows: {}, in: {}ms", batchCount, totalRowCount, TimeUnit.MILLISECONDS.convert(System.nanoTime() - time, TimeUnit.NANOSECONDS));
                }
            }
            catch (SQLException e)
            {
                connection.rollback();
                throw new RuntimeException(e);
            }
            finally
            {
                connection.setAutoCommit(true);
            }
        }
        catch (SQLException e)
        {
            throw new RuntimeException(e);
        }
        finally
        {
            input.close();
            context.getSession()
                    .unregisterAbortListener(abortListener);
        }
    }

    private String createTableAndPreparedStatement(SqlDialect sqlDialect, Connection connection, Schema schema) throws SQLException
    {
        String identifierQuoteString = sqlDialect.getIdentifierQuoteString(connection);
        // Select into
        if (insertIntoColumns.isEmpty())
        {
            String createTableStm = JdbcUtils.getCreateTableStatement(sqlDialect, table, schema, identifierQuoteString, columnOptions);
            LOGGER.debug("Create table: {}", createTableStm);
            try (PreparedStatement stm = connection.prepareStatement(createTableStm))
            {
                stm.execute();
            }
        }

        StringBuilder sb = new StringBuilder();
        sb.append("INSERT INTO ")
                .append(table.getParts()
                        .stream()
                        .map(p -> "%1$s%2$s%1$s".formatted(identifierQuoteString, p))
                        .collect(joining(".")))
                .append("\n(\n");
        if (insertIntoColumns.isEmpty())
        {
            sb.append(schema.getColumns()
                    .stream()
                    .map(c -> "%1$s%2$s%1$s".formatted(identifierQuoteString, c.getName()))
                    .collect(joining(",\n")));
        }
        else
        {
            sb.append(insertIntoColumns.stream()
                    .map(c -> "%1$s%2$s%1$s".formatted(identifierQuoteString, c))
                    .collect(joining(",\n")));
        }
        sb.append(")\n");
        sb.append("VALUES (");
        sb.append(IntStream.range(0, schema.getSize())
                .mapToObj(i -> "?")
                .collect(joining(", ")));
        sb.append(")\n");
        LOGGER.debug("{}: {}", insertIntoColumns.isEmpty() ? "SELECT INTO"
                : "INSEERT INTO", sb);

        return sb.toString();
    }
}
