<a href="https://typesense.org"><img src="typesense_medium.png?raw=true" alt="Typesense" width="298" /></a>

Typesense is a fast, typo-tolerant search engine for building delightful search experiences.

## Features

- **Typo tolerant:** Handles typographical errors elegantly.
- **Simple and delightful:** Simple to set-up and manage.
- **Tunable ranking:** Easy to tailor your search results to perfection.
- **Fast:** Meticulously designed and optimized for speed.

## Install

You can download the [binary packages](https://github.com/wreally/typesense/releases) that we publish for 
Linux (x86-64) and Mac.

You can also run Typesense from our [official Docker image](https://hub.docker.com/r/typesense/typesense/):

```
docker run -p 8108:8108 -v/tmp/typesense-data:/data typesense/typesense:0.8.0 --data-dir /data --api-key=Hu52dwsas2AdxdE --listen-port 8108
```

## Quick Start

Here's a quick example showcasing how you can create a collection, index a document and search it on Typesense. 

Let's initialize the client and create a `companies` collection:

```python
import typesense

typesense.master_node = typesense.Node(
    host='localhost', 
    port=8108, 
    protocol='http', 
    api_key='<YOUR_API_KEY>'
)

typesense.Collections.create({
  "name": "companies",
  "fields": [
    {"name": "company_name", "type": "string" },
    {"name": "num_employees", "type": "int32" },
    {"name": "country", "type": "string", "facet": true }
  ],
  "token_ranking_field": "num_employees"
})
```

Now, let's add a document to the collection we just created:

```python
document = {
 "id": "124",
 "company_name": "Stark Industries",
 "num_employees": 5215,
 "country": "USA"
}

typesense.Documents.create('companies', document)
```

Finally, let's search for the document we just indexed:

```python
search_parameters = {
  'q'         : 'stork',
  'query_by'  : 'company_name',
  'filter_by' : 'num_employees:>100',
  'sort_by'   : 'num_employees:desc'
}

typesense.Documents.search('companies', search_parameters)
```

**Did you notice the typo in the query text?** No big deal. Typesense handles typographic errors out-of-the-box!

A detailed guide is available on [Typesense website](https://typesense.org/docs). 

## Building from source

**Building on your machine**

Typesense requires the following dependencies: 

* Snappy
* zlib
* OpenSSL (>=1.0.2)
* curl
* Compiler with good C++11 support (GCC >= 4.9.0, Apple Clang >= 8.0, Clang >= 3.9.0)

```
$ ./build.sh [--clean] [--depclean]
```

Other dependencies are pulled and built as part of the build process.

**Building a Docker image**

The Docker build script takes care of pulling the required dependencies, so you don't need to install them separately:

```
$ TYPESENSE_IMG_VERSION=nightly ./docker-build.sh --build-deploy-image [--clean] [--depclean]
```

&copy; 2016-2018 Wreally Studios Inc.