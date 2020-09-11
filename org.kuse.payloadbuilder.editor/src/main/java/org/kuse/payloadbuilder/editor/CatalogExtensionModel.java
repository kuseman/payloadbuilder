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
package org.kuse.payloadbuilder.editor;

/** Model for a catalog extension */
class CatalogExtensionModel
{
    private boolean enabled = true;
    private String alias;
    
    CatalogExtensionModel(String alias)
    {
        this.alias = alias;
    }
    
    boolean isEnabled()
    {
        return enabled;
    }
    
    void setEnabled(boolean enabled)
    {
        this.enabled = enabled;
    }
    
    String getAlias()
    {
        return alias;
    }
    
    void setAlias(String alias)
    {
        this.alias = alias;
    }
}
