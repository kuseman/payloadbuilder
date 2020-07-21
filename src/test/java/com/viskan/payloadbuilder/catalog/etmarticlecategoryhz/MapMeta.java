package com.viskan.payloadbuilder.catalog.etmarticlecategoryhz;

import java.io.Serializable;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Meta for a map */
public class MapMeta implements Serializable
{
    public static final long serialVersionUID = 1L;
    
    private final String[] columns;
    
    @JsonCreator
    public MapMeta(@JsonProperty("columns") String[] columns)
    {
        this.columns = columns;
    }
    
    public String[] getColumns()
    {
        return columns;
    }
}
