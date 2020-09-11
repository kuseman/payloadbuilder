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
package org.kuse.payloadbuilder.core.codegen;

import static java.util.Collections.emptySet;

import java.util.HashSet;
import java.util.Set;

/** Structure used when building code */
public class ExpressionCode
{
    private ExpressionCode()
    {
    }

    private String code = "";
    private String resVar;
    private String isNull;
    private Set<String> imports;

    public String getResVar()
    {
        return resVar;
    }

    public String getIsNull()
    {
        return isNull;
    }

    public String getCode()
    {
        return code;
    }

    public void setCode(String code)
    {
        this.code = code;
    }

    public void addImport(String imp)
    {
        if (imports == null)
        {
            imports = new HashSet<>();
        }
        imports.add(imp);
    }

    public void addImports(Set<String> imports)
    {
        if (imports == null)
        {
            this.imports = new HashSet<>(imports);
        }
        else
        {
            this.imports.addAll(imports);
        }
    }

    public Set<String> getImports()
    {
        return imports != null ? imports : emptySet();
    }

    public static ExpressionCode code(CodeGeneratorContext context)
    {
        return code(context, null);
    }

    /**
     * Create a new code from context Generates a new result and isnull variable
     **/
    public static ExpressionCode code(CodeGeneratorContext context, ExpressionCode parent)
    {
        ExpressionCode ec = new ExpressionCode();
        ec.resVar = context.newVar("res");
        ec.isNull = context.newVar("isNull");

        if (parent != null)
        {
            ec.imports = parent.imports;
        }

        return ec;
    }
}
