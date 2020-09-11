/**
 *
 *  Copyright (c) Marcus Henriksson <kuseman80@gmail.com>
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package org.kuse.payloadbuilder.core.parser;

import org.apache.commons.lang3.NotImplementedException;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;

/** Base class for expressions */
public abstract class Expression
{
    public abstract <TR, TC> TR accept(ExpressionVisitor<TR, TC> visitor, TC context);

    /** Returns true if the result from this expression is nullable. Primitive result or not */
    public abstract boolean isNullable();

    public Class<?> getDataType()
    {
        return Object.class;
    }

    /** Fold's expression */
    public Expression fold()
    {
        return this;
    }

    /** Returns true if this expression is constant or not */
    public boolean isConstant()
    {
        return false;
    }

    /**
     * Generate code for this expression
     *
     * @param context Context used during code generation
     * @param parentCode Code generated from parent expression
     **/
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode)
    {
        throw new NotImplementedException("code gen: " + getClass().getSimpleName());
    }

    /**
     * Evaluate this expression with context
     *
     * @param context Context used during evaluation
     */
    public Object eval(ExecutionContext context)
    {
        throw new NotImplementedException("Not implemented. eval: " + getClass().getSimpleName());
    }
}
