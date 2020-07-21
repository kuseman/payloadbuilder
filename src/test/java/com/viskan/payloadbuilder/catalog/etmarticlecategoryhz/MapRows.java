package com.viskan.payloadbuilder.catalog.etmarticlecategoryhz;

import java.util.List;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/** Rows contained in a map */
public class MapRows
{
    private final List<Object[]> rows;
   
    @JsonCreator
    public MapRows(@JsonProperty("rows") List<Object[]> rows)
    {
        this.rows = rows;
    }
    
    public List<Object[]> getRows()
    {
        return rows;
    }
}
