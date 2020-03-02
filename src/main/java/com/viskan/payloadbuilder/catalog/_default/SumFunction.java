package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.FunctionInfo;

/** Sums input */
class SumFunction extends FunctionInfo
{
    SumFunction(Catalog catalog)
    {
        super(catalog, "sum");
    }
}
