package com.viskan.payloadbuilder.catalog._default;

import com.viskan.payloadbuilder.catalog.Catalog;
import com.viskan.payloadbuilder.catalog.ScalarFunctionInfo;

/** Sums input */
class SumFunction extends ScalarFunctionInfo
{
    SumFunction(Catalog catalog)
    {
        super(catalog, "sum");
    }
}
