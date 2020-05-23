package com.viskan.payloadbuilder.catalog;

import com.viskan.payloadbuilder.TableAlias;
import com.viskan.payloadbuilder.operator.BatchOperator;
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
    
    /*
     * from source s
     * inner join article a
     *   on  a.art_id = s.art_id
     *   and a.active_flg
     *  
     *  
     *  outer = getOperator(s, null)
     *  inner = getOperator(a, "a.art_id = s.art_id and a.active_flg")
     *  
     *  
     *  BatchMergeJoin (a.art_id = s.art_id)
     *    scan(source)
     *    filter(a.active_flg)
     *      batch(article)
     *        
     *  if (hashJoin)
     *  {
     *    if (joinCondition satisfies primary key on inner AND outer)
     *      join = BatchMergeJoin
     *    else if (joinCondition satisfies primary key on inner OR outer)
     *      join = BatchHashJoin
     *    else
     *      join = HashJoin
     *  }
     *  else
     *   join = nestedLoop
     *    
     *  Primary key
     *   - All key columns must be present to utilize BatchOperator
     *     (Might be added later on for catalog to support batching of partial primary key)
     *   - BatchMergeJoin
     *     Join condition must utilize primary key (full or partial) on both sides
     *   - BatchHashJoin
     *     If join condition only utilizes primary key on one side
     *     
     *    
     *  BatchMergeJoin
     *   --   
     *    
     *  from source s
     *  inner join article_attribute aa
     *    on  aa.art_id = s.art_id
     *  inner join attribute1 a1
     *    on a1.attr1_id = aa.attr1_id
     *   
     *   
     *  BatchMergeJoin (aa.art_id = s.art_id)
     *    scan(source)
     *    BatchHashJoin (a1.attr1_id = aa.attr1_id)
     *      batch(article_attribute)
     *      batch(attribute1)
     *  
     *                     (Primary key)
     *  aa (attr1_id)      a1 (attr1_id)
     *  1                  1
     *  10                 10
     *  4                  4
     *  3                  3
     *  10          
     *  
     *  1 Read aa rows
     *  2 Batch a1 rows (optimization, keep track of already read a1 keys)
     *  3 Hash a1 rows
     *  4 Probe read aa rows and join
     *  5 Loop 1
     *  
     *  
     *          
     */
    
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
    public Operator getScanOperator(TableAlias alias)
    {
        throw new IllegalArgumentException("Catalog " + name + " doesn't support scan operators.");
    }
    
    /** Get batch reader for provided table.
     * Used in {@link BatchOperator}
     * @param index Index for reader
     * */
    public BatchOperator.Reader getBatchReader(QualifiedName table, Index index)
    {
        throw new IllegalArgumentException("Catalog " + name + " doesn't support batch operator. Check implementation, catalog returned index for " + table);
    }
    
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
