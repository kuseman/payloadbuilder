package com.viskan.payloadbuilder.codegen;

import com.viskan.payloadbuilder.Row;

import java.util.function.Function;

/** Base class for generated functions */
public abstract class BaseFunction extends BaseExpression implements Function<Row, Object>
{
}
