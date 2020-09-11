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
package org.kuse.payloadbuilder.core.catalog.builtin;

import static java.util.stream.Collectors.joining;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

import org.kuse.payloadbuilder.core.catalog.Catalog;
import org.kuse.payloadbuilder.core.catalog.ScalarFunctionInfo;
import org.kuse.payloadbuilder.core.codegen.CodeGeneratorContext;
import org.kuse.payloadbuilder.core.codegen.ExpressionCode;
import org.kuse.payloadbuilder.core.parser.ExecutionContext;
import org.kuse.payloadbuilder.core.parser.Expression;

/** Hash function. Hahses arguments using Objects.hash */
class HashFunction extends ScalarFunctionInfo
{
    HashFunction(Catalog catalog)
    {
        super(catalog, "hash");
    }

    @Override
    public Class<?> getDataType()
    {
        return int.class;
    }

    @Override
    public Object eval(ExecutionContext context, List<Expression> arguments)
    {
        int size = arguments.size();
        Object[] args = new Object[size];
        for (int i = 0; i < size; i++)
        {
            args[i] = arguments.get(i).eval(context);
            if (args[i] == null)
            {
                return null;
            }
        }
        return Objects.hash(args);
    }

    @Override
    public ExpressionCode generateCode(
            CodeGeneratorContext context,
            ExpressionCode parentCode,
            List<Expression> arguments)
    {
        ExpressionCode code = ExpressionCode.code(context);

        StringBuilder sb = new StringBuilder();
        List<String> argVars = new ArrayList<>();
        List<String> isNullVars = new ArrayList<>();

        for (Expression arg : arguments)
        {
            ExpressionCode argCode = arg.generateCode(context, parentCode);
            sb.append(argCode.getCode());
            argVars.add(argCode.getResVar());
            isNullVars.add(argCode.getIsNull());
        }

        String template = "%s"
            + "boolean %s = true;\n"
            + "int %s = -1;\n"
            + "if (!(%s))\n"
            + "{\n"
            + "  %s = java.util.Objects.hash(%s);\n"
            + "  %s = false;\n"
            + "}\n";

        code.setCode(String.format(template,
                sb.toString(),
                code.getIsNull(),
                code.getResVar(),
                isNullVars.stream().collect(joining("||")),
                code.getResVar(), argVars.stream().collect(joining(",")),
                code.getIsNull()));

        return code;
    }
}
