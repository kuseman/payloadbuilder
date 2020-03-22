package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;

/** Definition of an operator factory */
public interface OperatorFactory
{
    /** Create operator for provided table */
    Operator create(TableAlias tableAlias);
}
