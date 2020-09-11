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
package org.kuse.payloadbuilder.core.catalog;

import java.util.List;
import java.util.Set;
import java.util.function.Function;

import org.apache.commons.lang3.NotImplementedException;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.operator.TableAlias;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Definition of a scalar function */
public abstract class ScalarFunctionInfo extends FunctionInfo
{
    public ScalarFunctionInfo(Catalog catalog, String name)
    {
        super(catalog, name, Type.SCALAR);
    }

    /**
     * Resolves resulting aliases for this function for provided parent aliases.
     *
     * <pre>
     * Example:
     * "concat(aa, aa.ap)"
     * This has source as parent alias and will resolve both arguments as resulting aliases
     * [aa, ap]
     *
     * Example:
     * "aa.filter(aa, x -> x.sku_id > 0)"
     * Resulting alias of a filter is the first arg alias ie. [aa]
     * </pre>
     *
     * @param parentAliases Parent aliases in context to this function
     * @param arguments Argument expressions to this function
     * @param aliasResolver Resolver for resolving arguments aliases
     **/
    public Set<TableAlias> resolveAlias(Set<TableAlias> parentAliases, List<Expression> arguments, Function<Expression, Set<TableAlias>> aliasResolver)
    {
        return parentAliases;
    }

    /** Evaluate this function */
    @SuppressWarnings("unused")
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        throw new NotImplementedException("eval: " + getClass().getSimpleName());
    }

    /**
     * Generate code for this function. Default is fallback to eval.
     *
     * @param context Context used during evaluation
     * @param parentCode Expression code from parent node
     * @param arguments Arguments to function
     **/
    public ExpressionCode generateCode(CodeGeneratorContext context, ExpressionCode parentCode, List<Expression> arguments)
    {
        Class<?> clazz = getClass();
        if (clazz.isAnonymousClass())
        {
            clazz = clazz.getSuperclass();
        }
        throw new NotImplementedException("generate code: " + clazz.getSimpleName());
    }
}
