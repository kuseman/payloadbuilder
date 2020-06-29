package com.viskan.payloadbuilder.editor.catalog;

import com.viskan.payloadbuilder.editor.catalog.Property.Presentation;

import static java.util.Collections.emptyList;
import static java.util.stream.Collectors.toList;
import static org.apache.commons.lang3.StringUtils.isBlank;

import java.beans.PropertyChangeListener;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;

import javax.swing.event.SwingPropertyChangeSupport;

import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpGet;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

import com.fasterxml.jackson.databind.ObjectMapper;

/** Extension for EtmArticleCategory (V2) data located in ES */
public class EtmArticleCategoryESCatalogExtension implements ICatalogExtension
{
    private static final ObjectMapper MAPPER = new ObjectMapper();
    private static final String INDEXES = "indexes";
    private static final String RELOAD = "reload";
    private static final String NEWENDPOINT = "newendpoint";
    private static final String ENDPOINT = "endpoint";
    private static final String ENDPOINTS = "endpoints";
    private static final String INDEX = "index";
    private static final CloseableHttpClient CLIENT = HttpClientBuilder.create().build();

    private final SwingPropertyChangeSupport pcs = new SwingPropertyChangeSupport(this, true);

    private final List<String> endpoints = new ArrayList<>();
    private String newEndpoint = "";

    private String endpoint = "";// "http://es-stage.viskan.com:9200, http://elasticsearch3x.viskans.loc";
    private EsIndex index = null;

    @Override
    public String getTitle()
    {
        return "EtmArticleCategory ES";
    }
    
    @Override
    public String getDefaultAlias()
    {
        return "etmes";
    }

    @Override
    public void addPropertyChangeListener(PropertyChangeListener listener)
    {
        pcs.addPropertyChangeListener(listener);
    }

    @Override
    public void removePropertyChangeListener(PropertyChangeListener listener)
    {
        pcs.removePropertyChangeListener(listener);
    }

    @Property(sort = 10, title = "New endpoint", tooltip = "Add new ES endpoint", name = NEWENDPOINT, presentation = Presentation.STRING, group = "Config")
    public String getNewEndpoint()
    {
        return newEndpoint;
    }

    public void setNewEndpoint(String newEndpoint)
    {
        String oldValue = this.newEndpoint;
        String newValue = newEndpoint;
        if (!Objects.equals(oldValue, newValue))
        {
            this.newEndpoint = newEndpoint;
            pcs.firePropertyChange(NEWENDPOINT, oldValue, newValue);
        }
    }

    @Property(sort = 11, title = "Add", name = NEWENDPOINT, presentation = Presentation.ACTION, group = "Config")
    public void addNewEndpoint()
    {
        if (isBlank(newEndpoint))
        {
            return;
        }

        endpoints.add(newEndpoint);
        setNewEndpoint("");

        pcs.firePropertyChange(ENDPOINTS, null, endpoints);
    }

    @Property(sort = 12, title = "Endpoint", tooltip = "Endpoint to ES", name = ENDPOINT, presentation = Presentation.LIST, itemsPropertyName = ENDPOINTS, group = "Config")
    public String getEndpoint()
    {
        return endpoint;
    }

    public void setEndpoint(String endpoint)
    {
        this.endpoint = endpoint;
    }

    @Property(sort = 13, title = "Remove", name = "remove", presentation = Presentation.ACTION, group = "Config")
    public void removeEndpoint()
    {
        if (isBlank(endpoint))
        {
            return;
        }

        endpoints.remove(endpoint);
        setEndpoint("");

        pcs.firePropertyChange(ENDPOINTS, null, endpoints);
    }

    @Property(sort = 4, title = "Index", presentation = Presentation.LIST, name = INDEX, itemsPropertyName = INDEXES, itemsListSize = 50, group = "Index")
    public EsIndex getIndex()
    {
        return index;
    }

    public void setIndex(EsIndex index)
    {
        EsIndex oldValue = this.index;
        EsIndex newValue = index;
        if (!Objects.equals(oldValue, newValue))
        {
            this.index = index;
            pcs.firePropertyChange(INDEX, oldValue, newValue);
        }
    }

    @Property(sort = 5, title = "Reload indexes", presentation = Presentation.ACTION, name = RELOAD, group = "Index")
    public void reloadIndexes()
    {
        if (endpoints.isEmpty())
        {
            return;
        }
        List<EsIndex> result = new ArrayList<>();
        for (String endpoint : endpoints)
        {
            HttpGet get = new HttpGet(endpoint + "/_mappings");
            try (CloseableHttpResponse response = CLIENT.execute(get))
            {
                Map<String, Object> indices = MAPPER.readValue(response.getEntity().getContent(), Map.class);
                result.addAll(indices.keySet()
                        .stream()
                        .map(i -> new EsIndex(endpoint, i))
                        .collect(toList()));
            }
            catch (IOException e)
            {
                // TODO: logging
                System.out.println("Error fetching indices: " + e);
            }
        }

        result.removeIf(i -> !i.index.startsWith("ramos") || !i.index.endsWith("_v3"));
        result.sort((a, b) -> String.CASE_INSENSITIVE_ORDER.compare(a.index, b.index));

        pcs.firePropertyChange(INDEXES, emptyList(), result);
    }

    /** Class representing a endpoint/index combo */
    private static class EsIndex
    {
        private final String endpoint;
        private final String index;

        EsIndex(String endpoint, String index)
        {
            this.endpoint = endpoint;
            this.index = index;
        }

        @Override
        public String toString()
        {
            return index;
        }
    }
}
