# Typesense: Design

## Motivation

Typesense's design is motivated by the following considerations:

- **Simplicity:** Typesense has to be super simple to set-up and get started with. The default configuration 
*should just work* for the common search use cases.
- **Typo-tolerance out-of-the-box:** Currently, it's not at all easy to build a typo-tolerant search using existing 
systems without a considerable speed/memory penalty. This has to change, given how common typographic errors are 
in the real-world.
- **In-memory:** All primary data structures would be in-memory, with the disk being used only for durability and for 
fetching large, unindexed fields.
- **Optimize for reads over writes:** A typical search engine is written once and read many times. The system should be 
cognizant of this read/write pattern.
- **Fast, without sacrificing relevance:** While speed is important, one cannot compromise on the quality of results 
returned. Remember that the average reaction time for humans is 200ms to a visual stimulus.
- **Laser focused on search:** While there might be some overlap with what a relational database does, strive to focus 
primarily on search, instead of becoming a generalized data store with a complex query language.
- **Availability over consistency**: In the event of a partition failure, it's better to give slightly stale search 
results, than being unavailable. This is alright, given the inherent asynchronous nature of the indexing process.

## Overview

- At the heart of Typesense is a `token => documents` inverted index backed by an 
[Adapative Radix Tree](https://db.in.tum.de/~leis/papers/ART.pdf), which is a memory-efficient implementation of the 
Trie data structure. ART allows us to do fast fuzzy searches on a query.
- Typesense consumes JSON documents as input. Fields that should be indexed must be specified via a configuration file 
  or through the API.
- The raw JSON documents are stored on disk using RocksDB. SSD disks are highly recommended.
- Search results are ranked on the following factors:
    - Number of matching tokens
    - Proximity of search tokens within the documents that contain these tokens
    - User specified static score for a document (for e.g. the number of followers could a static score for a 
      Twitter user)
- A typical search query involves:
    - a search term (required - wild card `*` search is not allowed)
    - filter fields (optional)
    - facet fields (optional)
    - sort fields (optional)
    - page
    - limit
- Typesense is exposed through a RESTful API, so that it can be consumed directly by web apps via AJAX requests.
- High Availability is achieved using a single-master, multiple read-only replica set-up. The read-only replicas 
  asynchronously pull data from master. The API clients automatically failover reads to the replicas if the master 
  is unavailable.