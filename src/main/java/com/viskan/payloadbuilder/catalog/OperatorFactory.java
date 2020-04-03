package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;

/** Definition of an operator factory */
public interface OperatorFactory
{
    /** Create operator for provided table */
    Operator create(QualifiedName qname, TableAlias tableAlias);
    
    /** Returns true if operator for provided qualified names requires
     * parent rows prior to fetching data. */
    boolean requiresParents(QualifiedName qname);
    
    /** Returns true if operator for provided qualified name supports predicates 
     * @param qname Qualified name
     **/
    default boolean supportsPredicates(QualifiedName qname)
    {
        return false;
    }
}
