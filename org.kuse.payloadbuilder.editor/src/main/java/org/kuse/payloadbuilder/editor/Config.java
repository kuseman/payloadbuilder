package org.kuse.payloadbuilder.editor;

import static java.util.Collections.emptyList;

import java.io.File;
import java.lang.reflect.Constructor;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Editor config */
class Config
{
    private static final int MAX_RECENT_FILES = 10;
    @JsonProperty
    private List<Catalog> catalogs;
    @JsonProperty
    private String lastOpenPath;
    @JsonProperty
    private final List<String> recentFiles = new ArrayList<>();

    List<Catalog> getCatalogs()
    {
        if (catalogs == null)
        {
            return emptyList();
        }
        return catalogs;
    }

    String getLastOpenPath()
    {
        return lastOpenPath;
    }

    void setLastOpenPath(String lastOpenPath)
    {
        this.lastOpenPath = lastOpenPath;
    }

    List<String> getRecentFiles()
    {
        return recentFiles;
    }

    void appendRecentFile(String file)
    {
        recentFiles.remove(file);
        recentFiles.add(0, file);
        if (recentFiles.size() > MAX_RECENT_FILES)
        {
            recentFiles.remove(recentFiles.size() - 1);
        }
    }

    void initExtensions()
    {
        if (catalogs == null)
        {
            return;
        }

        Map<String, URLClassLoader> classLoaderByJar = new HashMap<>();

        int size = catalogs.size();
        for (int i = size - 1; i >= 0; i--)
        {
            Catalog catalog = catalogs.get(i);
            try
            {
                URLClassLoader cl = classLoaderByJar.get(catalog.getJar());
                if (cl == null)
                {
                    URL jar = new File(catalog.getJar()).toURI().toURL();
                    cl = new URLClassLoader(new URL[] {jar}, getClass().getClassLoader());
                    classLoaderByJar.put(catalog.getJar(), cl);
                }

                Class<?> clazz = cl.loadClass(catalog.getClassName());
                if (ICatalogExtension.class.isAssignableFrom(clazz))
                {
                    Constructor<?> ctor = clazz.getDeclaredConstructors()[0];
                    ctor.setAccessible(true);
                    catalog.setCatalogExtension((ICatalogExtension) ctor.newInstance());
                }
                else
                {
                    System.err.println("Class: " + clazz + " does not implement " + ICatalogExtension.class);
                }
            }
            catch (Exception e)
            {
                // Remove bad catalog
                catalogs.remove(i);

                // TODO: log bus
                System.err.println("Cannot instantiate extensions from jar: " + catalog.getJar() + ", exception: " + e);
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
