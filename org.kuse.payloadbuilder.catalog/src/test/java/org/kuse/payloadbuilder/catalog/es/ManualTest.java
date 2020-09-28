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
package org.kuse.payloadbuilder.catalog.es;

import org.junit.Assert;
import org.junit.Ignore;
import org.junit.Test;
import org.kuse.payloadbuilder.core.Payloadbuilder;
import org.kuse.payloadbuilder.core.QueryResult;
import org.kuse.payloadbuilder.core.QuerySession;
import org.kuse.payloadbuilder.core.catalog.CatalogRegistry;

@Ignore
public class ManualTest extends Assert
{
    @Test
    public void test()
    {
        QuerySession session = new QuerySession(new CatalogRegistry());
        session.setPrintStream(System.out);
        session.getCatalogRegistry().registerCatalog("es", new ESCatalog());
        session.setDefaultCatalog("es");

      session.setCatalogProperty("es", "endpoint", "http://elasticsearch3x.viskans.loc");
//        session.setCatalogProperty("es", "endpoint", "http://elasticsearch.viskanint.local");
//        session.setCatalogProperty("es", "index", "ramossportshopentestcust_c0_store");

        String query = "select concat('{\"delete\":{\"_index\":\"', __index, '\",\"_type\":\"', __type, '\",\"_id\":\"', __id, '\"}}')\r\n" + 
            "from es#search(\r\n" + 
            "  index: 'ramoslager157prod_c0_v3',\r\n" + 
            "  body: '\r\n" + 
            "  scroll: true\r\n" +
            "  {\r\n" + 
            "  \"filter\": {\r\n" + 
            "    \"bool\": {\r\n" + 
            "      \"must\": [\r\n" + 
            "        {\r\n" + 
            "          \"term\": {\r\n" + 
            "            \"_docType\": \"EtmArticle\"\r\n" + 
            "          }\r\n" + 
            "        }\r\n" + 
            "      ],\r\n" + 
            "      \"must_not\": [\r\n" + 
            "        {\r\n" + 
            "          \"term\": {\r\n" + 
            "            \"_subType\": \"search\"\r\n" + 
            "          }\r\n" + 
            "        }\r\n" + 
            "      ]\r\n" + 
            "    }\r\n" + 
            "  }\r\n" + 
            "}\r\n" + 
            "  '\r\n" + 
            "\r\n" + 
            ")";

        QueryResult queryResult = Payloadbuilder.query(session, query);
        
//        FileOutputStream fis = new FileOutputStream(new File("c:/temp/clean_sportshopen_es.sh"));
//        JsonStringWriter writer = new JsonStringWriter();
        while (queryResult.hasMoreResults())
        {
//            queryResult.writeResult(writer);
//            System.out.println(writer.getAndReset());
        }
    }
}