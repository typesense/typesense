# Typesense: TODO

- Test for group + multiple fields
- Intersect with single posting list
- Test for erase dropping elements below compressed list threshold
- Test for array token positions

**Search index**

- ~~Fix memory ratio (decreasing with indexing)~~
- ~~Speed up wildcard searches further~~
- ~~Allow int64 in default sorting field~~
- ~~Use connection timeout for CURL rather than request timeout~~
- ~~Async import~~
- ~~Highlight all matching fields~~
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
- ~~Index and search on multi-valued field~~
- ~~range search for art_int~~
- ~~Restore records as well on restart (like for meta)~~
- ~~drop collection should remove all records from the store~~
- ~~Multi-key binary search during scoring~~
- ~~Assumption that all tokens match for scoring is no longer true~~
- ~~Filters~~
- ~~Facets~~
- ~~Schema validation during insertion (missing fields + type errors)~~
- ~~Proper score field for ranking tokens~~
- ~~Throw errors when schema is broken~~
- ~~Desc/Asc ordering with tests~~
- ~~Found count is wrong~~
- ~~Filter query in the API~~
- ~~Facet limit (hardcode to top 10)~~
- ~~Deprecate old split function~~
- ~~Multiple facets not working~~
- ~~Search snippet with highlight~~
- ~~Snippet should only be around surrounding matching tokens~~
- ~~Proper pagination~~
- ~~Pagination parameter~~
- ~~Drop collection API~~
- ~~JSONP response~~
- ~~"error":"Not found." is sent when query has no hits~~
- ~~Fix API response codes~~
- ~~List all collections~~
- ~~Fetch an individual document~~
- ~~ID field should be a string: must validate~~
- ~~Number of records in collection~~
- ~~Test for asc/desc upper/lower casing~~
- ~~Test for search without any sort_by given~~
- ~~Test for collection creation validation~~
- ~~Test for delete document~~
- ~~art float search~~
- ~~When prefix=true, use default_sorting_field for token ordering only for last word~~
- ~~only last token should be prefix searched~~
- ~~Prefix-search strings should not be null terminated~~
- ~~sort results by float field~~
- ~~json::parse must be wrapped in try catch~~
- ~~Collection Manager collections map should store plain collection name~~
- ~~init_collection of Collection manager should probably take seq_id as param~~
- ~~node score should be int32, no longer uint16 like in document struct~~ 
- ~~Typo in prefix search~~
- ~~When field of "id" but not string, what happens?~~
- ~~test for num_documents~~
- ~~test for string filter comparison: title < "foo"~~
- ~~Test for sorted_array::indexOf when length is 0~~
- ~~Test for pagination~~
- ~~search_fields, sort_fields and facet fields should be combined~~
- ~~facet fields should be indexed verbatim~~
- ~~change "search_by" to "query_by"~~
- ~~during index_in_memory() validations should be front loaded~~
- ~~Support default sorting field being a float~~
- ~~https support~~
- ~~Validate before string to int conversion in the http api layer~~
- ~~art bool support~~
- ~~Export collection~~
- ~~get collection should show schema~~
- ~~API key should be allowed as a GET parameter also (for JSONP)~~
- ~~Don't crash when the data directory is not found~~
- ~~When the first sequence ID is not zero, bail out~~
- ~~Proper status code when sequence number to fetch is bad~~
- ~~Replica should be read-only~~
- ~~string_utils::tokenize should not have max length~~
- ~~handle hyphens (replace them)~~
- ~~clean special chars before indexing~~
- ~~Add docs/explanation around ranking calc~~
- ~~UTF-8 normalization~~
- ~~Use rocksdb batch put for atomic insertion~~
- ~~Proper logging~~
- ~~Handle store-get() not finding a key~~
- ~~Deprecate converting integer to string verbatim~~ 
- ~~Deprecate union type punning~~
- ~~Replica server should fail when pointed to "old" master~~
- ~~gzip compress responses~~
- ~~Have a LOG(ERROR) level~~
- ~~Handle SIGTERM which is sent when process is killed~~
- ~~Use snappy compression for storage~~
- ~~Fix exclude_scalar early returns~~
- ~~Fix result ids length during grouped overrides~~
- ~~Fix override grouping (collate_included_ids)~~
- ~~Test for overriding result on second page~~
- atleast 1 token match for proceeding with drop tokens
- support wildcard query with filters
- API for optimizing on disk storage
- Jemalloc
- Exact search 
- NOT operator support
- Log operations
- Parameterize replica's MAX_UPDATES_TO_SEND
- NOT operator support
- 64K token limit
- > INT32_MAX validation for float field
- highlight of string arrays?
- test for token ranking on float field
- test for float int field deletion during doc deletion
- Test for snippets
- Test for replication
- Query token ids should match query token ordering
- ID should not have "/"
- Group results by field
- Delete using range: https://github.com/facebook/rocksdb/wiki/Delete-A-Range-Of-Keys
- Test for string utils
- Prevent string copy during indexing
- Minimum results should be a variable instead of blindly going with max_results
- Handle searching for non-existing fields gracefully
- test for same match score but different primary, secondary attr
- Support nested fields via "."
- Support search operators like +, - etc.
- Space sensitivity
- Use bitmap index instead of compressed array for doc list?
- Primary_rank_scores and secondary_rank_scores hashmaps should be combined?
- d-ary heap?
- ~~topster: reject min heap value compare only when field is same~~
- ~~match index instead of match score~~

**API**

- Support the following operations:
    - ~~create a new index~~
    - ~~index a single document~~    
    - ~~delete a document by ID~~
    - ~~query an index~~       
    - ~~Drop an index~~
    - ~~fetch a document by ID~~

**Clustering**

- Sync every incoming write with another Typesense server

**Refactoring**

- ~~`token_count` in leaf is redundant: can be accessed from value~~
- ~~storing length in `offsets` is redundant: it can be found by looking up value of the next index in offset_index~~

**Tech debt**

- ~~Use GLOB file pattern for CMake (better IDE refactoring support)~~
- DRY index_int64_field* methods