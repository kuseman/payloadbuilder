package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.Operator;
import com.viskan.payloadbuilder.parser.tree.QualifiedName;

import static java.util.Collections.emptyList;
import static java.util.Objects.requireNonNull;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/** Catalog. Top schema type, which defines the hooking points
 * for retrieving data, functions etc. */
public abstract class Catalog
{
    /** Name of the catalog */
    private final String name;
    /** Functions */
    private final Map<String, FunctionInfo> functionByName = new HashMap<>();
    
//    private OperatorFactory operatorFactory;
    
    // Register operatorFactory by table name
    
    public Catalog(String name)
    {
        this.name = requireNonNull(name, "name");
    }
    
    public String getName()
    {
        return name;
    }
    
    /** Get indices for provided table
     * @param table Table to retrieve index for
     **/
    public List<Index> getIndices(QualifiedName table)
    {
        return emptyList();
    }
    
    /** Get operator for provided alias
     * @param alias Alias to retrieve operator for 
     * @param condition Condition used while fetching rows */
    public Operator getOperator(TableAlias alias)
    {
        throw new IllegalArgumentException("Catalog " + name + " doesn't support scan operators.");
    }
    
    /** Get batch reader for provided table.
     * Used in {@link BatchOperator}
     * @param index Index for reader
     * */
//    public BatchOperator.Reader getBatchReader(QualifiedName table, Index index)
//    {
//        throw new IllegalArgumentException("Catalog " + name + " doesn't support batch operator. Check implementation, catalog returned index for " + table);
//    }
    
    /*
     * Use case:
     *   "from article"
     *   -> ScanIndex
     * 
     * Use case:
     *   "from article where art_in (1,2,3,4)
     *   -> ScanIndex
     *   -> KeyIndex if condition satisfies
     *   
     *  Use case:
     *    "from source s inner join article a on a.art_id = s.art_id"
     *   -> NestedLoop join
     *   -> BatchedHashMatch
     *   
     *  Index
     *    String[] keys  // art_id, country_id
     *    
     *  If there is an index and keys is satisfied, then always push outer rows
     *  into context before opening inner
     *  Only supports hashmatch
     *    
     *    Can I use HashMatch?
     *      - Join condition is met?
     *      
     *    HashMatch
     *      Outer
     *      index.createOperator()  
     *   
     *  Index creates operators
     *    -- Need to know join, inner, outer
     *  
     *  OperatorBuilder creates operators
     *    -- Need meta data from Index to choose correct
     *    -- NestedLoop, nothing is needed
     *    -- HashMatch, condition must satisfy
     *        batching
     *        hash left/right
     *        
     *   
     *  visit(Join)
     *    indices = catalog.getIndices(alias);
     *    Operator
     *    if (index.satisfies(condition))  
     */
    
//    public OperatorFactory getOperatorFactory()
//    {
//        return operatorFactory;
//    }
//    
//    public void setOperatorFactory(OperatorFactory operatorFactory)
//    {
//        this.operatorFactory = operatorFactory;
//    }

    /** Register function */
    public void registerFunction(FunctionInfo functionInfo)
    {
        requireNonNull(functionInfo);
        functionByName.put(functionInfo.getName().toLowerCase(), functionInfo);
    }

    /** Get function info by name */
    public FunctionInfo getFunction(String name)
    {
        return functionByName.get(requireNonNull(name).toLowerCase());
    }
}
