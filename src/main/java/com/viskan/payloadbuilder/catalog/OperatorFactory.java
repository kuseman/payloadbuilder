package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;

/** Definition of an operator factory */
public interface OperatorFactory
{
    /** Create operator for provided table */
    Operator create(QualifiedName qname, TableAlias tableAlias);
}
