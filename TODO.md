# Typesense: TODO

## Pre-alpha

**Search index**

- ~~Proper JSON as input~~
- ~~Storing raw JSON input to RocksDB~~
- ART for every indexed field
- UTF-8 support for fuzzy search
- Facets
- Filters
- Support search operators like +, - etc.

**API**

- Support the following operations:
    - create a new index
    - index a single document
    - bulk insert multiple documents
    - fetch a document by ID
    - delete a document by ID
    - query an index       

**Clustering**

- Sync every incoming write with another Typesense server

**Refactoring**

- ~~`token_count` in leaf is redundant: can be accessed from value~~
- ~~storing length in `offsets` is redundant: it can be found by looking up value of the next index in offset_index~~

**Tech debt**

- Use GLOB file pattern for CMake (better IDE refactoring support)