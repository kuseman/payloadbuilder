package com.viskan.payloadbuilder.provider.hazelcast;

import java.io.Serializable;
import java.time.LocalDateTime;

/** Meta for a map */
public class MapMeta implements Serializable
{
    public static final long serialVersionUID = 1L;
    
    private final String[] columns;
    private final LocalDateTime updateTime;
    private final LocalDateTime procedureModifiedTime;
    /** Change version that this map is at */
    private final long changeVersion;
    
    public MapMeta(String[] columns, long changeVersion)
    {
        this.columns = columns;
        this.updateTime = LocalDateTime.now();
        // TODO: Fix correct stamping of procedure date
        this.procedureModifiedTime = LocalDateTime.now();
        this.changeVersion = changeVersion;
    }
    
    public String[] getColumns()
    {
        return columns;
    }
    
    public LocalDateTime getUpdateTime()
    {
        return updateTime;
    }
    
    public long getChangeVersion()
    {
        return changeVersion;
    }
}
