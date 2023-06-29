package se.kuseman.payloadbuilder.core;

import static java.util.Objects.requireNonNull;

import java.util.List;

import se.kuseman.payloadbuilder.core.execution.QuerySession;
import se.kuseman.payloadbuilder.core.statement.QueryStatement;

/**
 * Result of a query compilation. This class can be reused between executions
 */
public class CompiledQuery
{
    private final QueryStatement query;
    private final List<Warning> warnings;

    CompiledQuery(QueryStatement query, List<Warning> warnings)
    {
        this.query = requireNonNull(query);
        this.warnings = warnings;
    }

    /** Execute this query with provided session */
    public QueryResult execute(QuerySession session)
    {
        return new QueryResultImpl(session, query);
    }

    public List<Warning> getWarnings()
    {
        return warnings;
    }

    /** A compiler warning */
    public static class Warning
    {
        private final String message;
        private int line;
        private int column;

        public Warning(String message, int line, int column)
        {
            this.message = message;
            this.line = line;
            this.column = column;
        }

        public String getMessage()
        {
            return message;
        }

        public int getLine()
        {
            return line;
        }

        public int getColumn()
        {
            return column;
        }
    }
}
