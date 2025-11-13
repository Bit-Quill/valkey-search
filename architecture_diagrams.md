# Valkey-Search Architecture Diagrams

## 1. High-Level Architecture Diagram

```mermaid
graph TB
    Client[Client Application]
    
    subgraph "Valkey Server"
        ValkeyCore[Valkey Core Engine]
        ModuleAPI[Module API Layer]
    end
    
    subgraph "Valkey-Search Module"
        ValkeySearch[ValkeySearch Main Class]
        Commands[Command Handlers]
        SchemaManager[Schema Manager]
        
        subgraph "Indexes"
            VectorIndex[Vector Indexes<br/>HNSW/Flat]
            NumericIndex[Numeric Index]
            TagIndex[Tag Index]
            TextIndex[Text Index]
        end
        
        subgraph "Query Engine"
            SearchEngine[Search Engine]
            FilterEngine[Filter Engine]
            ResponseGen[Response Generator]
        end
        
        subgraph "Coordinator (Cluster)"
            CoordServer[Coordinator Server]
            CoordClient[Coordinator Client]
        end
        
        ThreadPools[Thread Pools<br/>Reader/Writer]
    end
    
    subgraph "Storage"
        ValkeyData[(Valkey Data Store)]
        IndexData[(Index Data)]
    end
    
    Client -->|FT.SEARCH commands| ValkeyCore
    ValkeyCore -->|Module callbacks| ModuleAPI
    ModuleAPI -->|Command routing| ValkeySearch
    ValkeySearch --> Commands
    ValkeySearch --> ThreadPools
    
    Commands --> SchemaManager
    Commands --> SearchEngine
    
    SchemaManager --> VectorIndex
    SchemaManager --> NumericIndex  
    SchemaManager --> TagIndex
    SchemaManager --> TextIndex
    
    SearchEngine --> FilterEngine
    SearchEngine --> ResponseGen
    
    ValkeySearch --> CoordServer
    CoordServer --> CoordClient
    
    VectorIndex --> IndexData
    NumericIndex --> IndexData
    TagIndex --> IndexData
    TextIndex --> IndexData
    
    ValkeyCore --> ValkeyData
    
    style ValkeySearch fill:#e1f5fe
    style Commands fill:#f3e5f5
    style SearchEngine fill:#e8f5e8
```

## 2. Class Diagram - Command Implementation

```mermaid
classDiagram
    class ValkeyModuleCtx {
        <<Valkey API>>
    }
    
    class SearchParameters {
        +string index_schema_name
        +string attribute_alias
        +string query
        +int k
        +LimitParameter limit
        +SortByParameter sortby
        +bool no_content
        +vector~ReturnAttribute~ return_attributes
        +FilterParseResults filter_parse_results
    }
    
    class SortByParameter {
        +string field
        +SortOrder order
        +bool enabled
    }
    
    class QueryCommand {
        <<abstract>>
        +Execute(ctx, argv, argc, cmd)
        +ParseCommand(itr)* 
        +SendReply(ctx, neighbors)*
    }
    
    class SearchCommand {
        +ParseCommand(itr)
        +SendReply(ctx, neighbors)
        +IsNonVectorQuery()
    }
    
    class KeyValueParser~SearchParameters~ {
        +Parse(parameters, itr)
        +AddParamParser(name, parser)
    }
    
    class ParamParser~SearchParameters~ {
        +function~SearchParameters, ArgsIterator~
    }
    
    class IndexSchema {
        +GetIndex(alias)
        +GetIdentifier(alias)
        +GetAttributeDataType()
    }
    
    class SearchEngine {
        +Search(parameters, mode)
        +ApplySorting(results, parameters)
        +MaybeAddIndexedContent(results, parameters)
    }
    
    SearchParameters <|-- QueryCommand
    QueryCommand <|-- SearchCommand
    SearchCommand --> KeyValueParser
    KeyValueParser --> ParamParser
    SearchCommand --> IndexSchema
    SearchCommand --> SearchEngine
    SearchParameters --> SortByParameter
    
    note for SearchCommand "Implements SORTBY parsing\nand result sorting"
    note for SortByParameter "New: Added for SORTBY\nsupport with field and order"
```

## 3. Sequence Diagram - FT.SEARCH Command Flow

```mermaid
sequenceDiagram
    participant Client
    participant ValkeyCore
    participant ModuleAPI
    participant SearchCommand
    participant Parser
    participant IndexSchema
    participant SearchEngine
    participant Indexes
    participant ResponseGen
    
    Client->>ValkeyCore: FT.SEARCH myindex "query" SORTBY price DESC
    ValkeyCore->>ModuleAPI: Route to valkey-search module
    ModuleAPI->>SearchCommand: FTSearchCmd(ctx, argv, argc)
    
    SearchCommand->>SearchCommand: Create SearchCommand instance
    SearchCommand->>Parser: ParseCommand(args_iterator)
    
    Parser->>Parser: Parse index name and query
    Parser->>Parser: Parse SORTBY price DESC
    Parser->>Parser: Parse other parameters (LIMIT, etc.)
    Parser-->>SearchCommand: SearchParameters populated
    
    SearchCommand->>IndexSchema: GetIndex(index_name)
    IndexSchema-->>SearchCommand: IndexSchema instance
    
    SearchCommand->>SearchEngine: Search(parameters, LOCAL)
    
    SearchEngine->>SearchEngine: DoSearch(parameters)
    SearchEngine->>Indexes: Query indexes based on parameters
    Indexes-->>SearchEngine: Raw search results
    
    SearchEngine->>SearchEngine: MaybeAddIndexedContent(results)
    SearchEngine->>Indexes: Fetch attribute content for results
    Indexes-->>SearchEngine: Results with content
    
    SearchEngine->>SearchEngine: ApplySorting(results, parameters)
    Note over SearchEngine: NEW: Sort by price field DESC
    SearchEngine->>IndexSchema: GetIndex("price") 
    IndexSchema-->>SearchEngine: Numeric index for price
    SearchEngine->>SearchEngine: Sort results by price values
    SearchEngine-->>SearchCommand: Sorted results
    
    SearchCommand->>ResponseGen: SendReply(ctx, neighbors)
    ResponseGen->>ResponseGen: Apply LIMIT parameters
    ResponseGen->>ResponseGen: Format response array
    ResponseGen->>ValkeyCore: ValkeyModule_ReplyWithArray()
    
    ValkeyCore-->>Client: Sorted search results
    
    Note over SearchCommand,SearchEngine: SORTBY implementation adds<br/>sorting step after content retrieval<br/>but before response formatting
```

## Key Implementation Points

### SORTBY Integration:
1. **Parser Level**: Added `ConstructSortByParser()` to handle `SORTBY field [ASC|DESC]` syntax
2. **Data Structure**: Extended `SearchParameters` with `SortByParameter` 
3. **Execution**: Added `ApplySorting()` function called after content retrieval
4. **Validation**: Ensures sort field is indexed before attempting to sort

### Flow Characteristics:
- **Non-intrusive**: SORTBY doesn't affect search performance, only final result ordering
- **Type-aware**: Different sorting logic for numeric vs tag fields
- **Error-safe**: Graceful handling of missing values and unsupported field types
- **Compatible**: Works with existing LIMIT, RETURN, and other FT.SEARCH options
