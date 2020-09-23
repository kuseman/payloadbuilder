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
package org.kuse.payloadbuilder.core.test;

import java.util.List;

/** Definition of a test harness.
 * <pre>
 * Works by setting a test harness containng 
 * a set of catalog with their 
 *  
 *  </pre> 
 **/
class TestHarness
{
    private String name;
    private List<TestCatalog> catalogs;
    private List<TestCase> cases;
    
    String getName()
    {
        return name;
    }
    
    void setName(String name)
    {
        this.name = name;
    }
    
    List<TestCatalog> getCatalogs()
    {
        return catalogs;
    }

    void setCatalogs(List<TestCatalog> catalogs)
    {
        this.catalogs = catalogs;
    }
    
    List<TestCase> getCases()
    {
        return cases;
    }
    
    void setCases(List<TestCase> cases)
    {
        this.cases = cases;
    }
}
