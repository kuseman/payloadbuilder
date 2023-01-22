package se.kuseman.payloadbuilder.api.catalog;

import se.kuseman.payloadbuilder.api.QualifiedName;
import se.kuseman.payloadbuilder.api.expression.IComparisonExpression;
import se.kuseman.payloadbuilder.api.expression.IExpression;
import se.kuseman.payloadbuilder.api.expression.IExpressionVisitor;
import se.kuseman.payloadbuilder.api.expression.IFunctionCallExpression;
import se.kuseman.payloadbuilder.api.expression.IInExpression;
import se.kuseman.payloadbuilder.api.expression.ILikeExpression;
import se.kuseman.payloadbuilder.api.expression.INullPredicateExpression;

/**
 * Definition of a analyzed predicate.
 *
 * <pre>
 * This is the foundation of an analyzed predicate (ie. WHERE or JOIN condition).
 * Core analyzes a predicate and splits it into pairs. Common predicate types
 * are provided for easier process by catalogs. More complex predicates (nested AND/OR etc.)
 * are accessible via {@link #getUndefinedExpression()} and can be utilized via a custom. {@link IExpressionVisitor}
 *
 * Example:
 *
 *     a.col = b.col
 * AND a.col1 = '10'
 * AND a.col2 LIKE 'abc%'
 * AND a.col3 IN (1,2,3)
 * AND someFunc()
 * AND a.col4 IS NULL
 *
 * This will yield 4 analyzed pairs
 *
 *   - COMPARISON (EQ) =&gt; a.col1 = '10'
 *   - LIKE            =&gt; a.col2 LIKE 'abc%'
 *   - IN              =&gt; a.bol3 IN (1,2,3)
 *   - FUNCTION_CALL   =&gt; someFunc()
 *   - NULL            =&gt; a.col4 IS NULL
 *
 *  Catalogs can pick those types/columns that it supports and handle them at the data source
 *  level to avoid core to process to much data.
 * </pre>
 */
public interface IPredicate
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

    /** Return the qualified column name of this pair (if any exists). If no column is present null is returned */
    QualifiedName getQualifiedColumn();

    /**
     * Return comparison value expression. Only applicable if {@link #getType()} is {@link Type#COMPARISION}. This returns the "other" side of the comparison regarding the
     * {@link IPredicate#getQualifiedColumn()}.
     * 
     * <pre>
     * ie.
     * 
     *   10 &lt; col
     *   
     *   Here ILiteralIntegerExpression(10) will be returned
     * 
     * </pre>
     */
    IExpression getComparisonExpression();

    /** Return the comparison type. Only applicable if {@link #getType()} is {@link Type#COMPARISION} */
    IComparisonExpression.Type getComparisonType();

    /**
     * Return in expression. Only applicable if {@link #getType()} is {@link Type#IN}
     */
    IInExpression getInExpression();

    /** Return like expression. Only applicable if {@link #getType()} is {@link Type#LIKE} */
    ILikeExpression getLikeExpression();

    /** Return null predicate expression. Only applicable if {@link #getType()} is {@link Type#NULL} */
    INullPredicateExpression getNullPredicateExpression();

    /** Return function call expression. Only applicable if {@link #getType()} is {@link Type#FUNCTION_CALL} */
    IFunctionCallExpression getFunctionCallExpression();

    /** Return function call expression. Only applicable if {@link #getType()} is {@link Type#UNDEFINED} */
    IExpression getUndefinedExpression();

    /** Type of predicate */
    enum Type
    {
        /**
         * Comparison pair. One of {@link IComparisonExpression.Type} is used. Will be used if the predicate pair has a column expression on either side of the comparison operator
         */
        COMPARISION,

        /**
         * In pair. {@link IInExpression} is used. Will be used if the IN operand is a column expression. Ie. t.col IN (1,2,3,4)
         */
        IN,

        /**
         * Like pair. {@link ILikeExpression} is used Will be used if the LIKE operand is a column expression. Ie. t.col LIKE 'some string'
         */
        LIKE,

        /**
         * Null predicate. Will be used if the NULL operand is a column expression. Ie. t.col IS (NOT) NULL
         */
        NULL,

        /**
         * Function call predicate. Will be used if the predicate is a function call. Ie. function(1,2,3)
         */
        FUNCTION_CALL,

        /** Undefined type. No analyze could be made for this item. Could be a nested OR etc. Can be analyzed by catalog by using {@link IExpressionVisitor} for building a nested query etc. */
        UNDEFINED
    }
}
