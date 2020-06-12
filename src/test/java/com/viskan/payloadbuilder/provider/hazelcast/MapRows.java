package com.viskan.payloadbuilder.provider.hazelcast;

import static java.util.Collections.emptyList;

import java.io.Serializable;
import java.time.LocalDateTime;
import java.util.List;

/** Rows contained in a map */
public class MapRows implements Serializable
{
    public static final long serialVersionUID = 1L;
    
    public static final MapRows EMPTY = new MapRows(emptyList());
    
    private final List<Object[]> rows;
    private final LocalDateTime updateTime;
   
    public MapRows(List<Object[]> rows)
    {
        this.rows = rows;
        this.updateTime = LocalDateTime.now();
    }
    
    public List<Object[]> getRows()
    {
        return rows;
    }
    
    public LocalDateTime getUpdateTime()
    {
        return updateTime;
    }
}
