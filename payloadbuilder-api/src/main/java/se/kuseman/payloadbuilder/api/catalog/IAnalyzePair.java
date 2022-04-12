package se.kuseman.payloadbuilder.api.catalog;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;

/**
 * Definition of a analyzed predicate pair.
 *
 * <pre>
 * This is the foundation of an analyzed predicate (ie. WHERE or JOIN condition).
 * Core analyzes a predicate and splits it this is interface represent a splitted pair.
 *
 * Example predicate:
 *
 *     a.col = b.col
 * AND a.col1 = '10'
 * AND a.col2 LIKE 'abc%'
 * AND a.bol3 IN (1,2,3)
 * AND someFunc()
 *
 * This will yield 4 analyzed pairs
 *
 *   - COMPARISON (EQ) =&gt; a.col1 = '10'
 *   - LIKE            =&gt; a.col2 LIKE 'abc%'
 *   - IN              =&gt; a.bol3 IN (1,2,3)
 *   - UNDEFINED       =&gt; someFunc()
 *
 *  To be able to process ORDER BY's and predicates, Catalogs can
 *  pick those types that it supports and handle them at the datasource
 *  level to avoid core to process to much data.
 *
 * </pre>
 */
public interface IAnalyzePair
{
    /**
     * Return the SQL representation of this pair.
     * 
     * <pre>
     *  &lt;some expression&gt; = a.value
     *  Will return the SQL representation of '&lt;some expression&gt; = a.value'
     * </pre>
     */
    String getSqlRepresentation();

    /** Return the type of this pair */
    Type getType();

    /** Return the comparison type of this pair. Only applicable if {@link #getType()} is {@link Type#COMPARISION} */
    IComparisonExpression.Type getComparisonType();

    /** Return the qualified name of this pair for provided alias */
    QualifiedName getQualifiedName(String alias);

    /** Return the column of this pair for provided alias */
    String getColumn(String alias);

    /** Return comparison value expression for provided alias. Only applicable if {@link #getType()} is {@link Type#COMPARISION} */
    IExpression getComparisonExpression(String alias);

    /** Return in expression for provided alias. Only applicable if {@link #getType()} is {@link Type#IN} */
    IInExpression getInExpression(String alias);

    /** Return like expression for provided alias. Only applicable if {@link #getType()} is {@link Type#LIKE} */
    ILikeExpression getLikeExpression(String alias);

    /**
     * Return the expression for this pair with provided class. Only applicable if {@link #getType()} is {@link Type#UNDEFINED}. Returns null if the value expression is not of provided type
     */
    <T extends IExpression> T getUndefinedValueExpression(Class<T> clazz);

    /** Type of pair */
    enum Type
    {
        /** Comparison pair. One of {@link IComparisonExpression.Type} is used */
        COMPARISION,

        /** In pair. {@link IInExpression} is used */
        IN,

        /** Like pair. {@link ILikeExpression} is used */
        LIKE,

        /** Null predicate. */
        NULL,

        /** Not null predicate. */
        NOT_NULL,

        /** Undefined type. No analyze could be made for this item. Could be a function call etc. */
        UNDEFINED
    }
}
