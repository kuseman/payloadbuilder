package se.kuseman.payloadbuilder.api.expression;

/** Column reference expression */
public interface IColumnExpression extends IExpression
{
    /** Get the referenced column */
    String getColumn();

    /** Return ordinal of column if present otherwise -1 */
    int getOrdinal();
}
