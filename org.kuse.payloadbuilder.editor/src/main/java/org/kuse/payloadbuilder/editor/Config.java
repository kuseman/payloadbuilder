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

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.groupingBy;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Editor config */
class Config
{
    @JsonProperty
    private List<Catalog> catalogs;

    List<Catalog> getCatalogs()
    {
        if (catalogs == null)
        {
            return emptyList();
        }
        return catalogs;
    }

    void initExtensions()
    {
        if (catalogs == null)
        {
            return;
        }

        Map<String, List<Catalog>> catalogsByJar = catalogs.stream().collect(groupingBy(Catalog::getJar));

        for (Entry<String, List<Config.Catalog>> entry : catalogsByJar.entrySet())
        {
            try
            {
                URL jar = new File(entry.getKey()).toURI().toURL();
                @SuppressWarnings("resource")
                // Should not be closed, otherwise catalogs cannot instantiate new classes
                URLClassLoader cl = new URLClassLoader(new URL[] {jar}, getClass().getClassLoader());
                for (Config.Catalog catalog : entry.getValue())
                {
                    Class<?> clazz = cl.loadClass(catalog.getClassName());
                    if (ICatalogExtension.class.isAssignableFrom(clazz))
                    {
                        Constructor<?> ctor = clazz.getDeclaredConstructors()[0];
                        ctor.setAccessible(true);
                        catalog.setCatalogExtension((ICatalogExtension) ctor.newInstance());
                    }
                }
            }
            catch (Exception e)
            {
                // TODO: log bus
                throw new RuntimeException("Cannot instansiate extensions from jar: " + entry.getKey(), e);
            }
        }
    }

    /** Catalog extension */
    static class Catalog
    {
        @JsonProperty
        private String jar;
        @JsonProperty
        private String className;
        @JsonProperty
        private Map<String, Object> config;
        @JsonIgnore
        private ICatalogExtension catalogExtension;

        String getJar()
        {
            return jar;
        }

        String getClassName()
        {
            return className;
        }

        Map<String, Object> getConfig()
        {
            return config;
        }

        void setConfig(Map<String, Object> config)
        {
            this.config = config;
        }

        ICatalogExtension getCatalogExtension()
        {
            return catalogExtension;
        }

        void setCatalogExtension(ICatalogExtension catalogExtension)
        {
            this.catalogExtension = catalogExtension;
        }
    }
}
