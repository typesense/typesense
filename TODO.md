# Typesense: TODO

## Pre-alpha

**Search index**

- ~~Proper JSON as input~~
- ~~Storing raw JSON input to RocksDB~~
- ~~ART for every indexed field~~
- ~~Delete should remove from RocksDB~~
- ~~Speed up UUID generation~~
- ~~Make the search score computation customizable~~
- ~~art int search should support signed ints~~
- ~~Search across multiple fields~~
- ~~Have set inside topster itself~~
- ~~Persist next_seq_id~~
- ~~collection_id should be int, not string~~
- ~~API should return count~~
- ~~Fix documents.jsonl path in tests~~
- ~~Multi field search tests~~
- ~~storage key prefix should include collection name~~
- Index and search on multi-valued field
- Proper field for ranking tokens
- Support nested fields via "."
- ~~Restore records as well on restart (like for meta)~~
- ~~drop collection should remove all records from the store~~
- Pagination parameter
- UTF-8 support for fuzzy search
- Facets
- Filters
- Support search operators like +, - etc.
- Prefix-search strings should not be null terminated
- string_utils::tokenize should not have max length
- only last token should be prefix searched
- art float search
- Minimum results should be a variable instead of blindly going with max_results
- Benchmark with -ffast-math
- Space sensitivity
- Use bitmap index instead of forarray for doc list
- Throw errors when schema is broken
- Assumption that all tokens match for scoring is no longer true
- Primary_rank_scores and secondary_rank_scores hashmaps should be combined
- Proper logging
- clean special chars before indexing
- d-ary heap?
- range search for art_int
- test for same match score but different primary, secondary attr

**API**

- Support the following operations:
    - create a new index
    - ~~index a single document~~
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

- ~~Use GLOB file pattern for CMake (better IDE refactoring support)~~