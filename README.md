# Typesense

Typesense is an open source search engine for building delightful search experiences.

- **Typo tolerance:** Handles typographical errors out-of-the-box
- **Tunable ranking:** Tailor your search results to perfection
- **Blazing fast:** Meticulously designed and optimized for speed
- **Simple and delightful:** Simple API, delightful out-of-the-box experience

## Installation

We publish [binary packages](https://github.com/wreally/typesense/releases) for Linux and OS X.

If you use Docker, you can also run Typesense from our official Docker image:

```
docker run -p 8108:8108 -v/tmp/typesense-data:/data typesense/typesense:0.8 --data-dir /data --api-key=Hu52dwsas2AdxdE --listen-port 8108
```

## Getting started

A detailed getting started guide is on [Typesense website](https://typesense.org/intro). 

Here's a quick example showcasing how you would create a collection, index a document and search it on Typesense. 

Let's create a `companies` collection:

```
curl "${TYPESENSE_HOST}/collections" \
       -X POST \
       -H "Content-Type: application/json" \
       -H "X-TYPESENSE-API-KEY: ${TYPESENSE_API_KEY}" \
       -d '{
             "name": "companies",
             "fields": [
               {"name": "company_name", "type": "string" },
               {"name": "num_employees", "type": "int32" },
               {"name": "country", "type": "string", "facet": true }
             ],
             "token_ranking_field": "num_employees"
          }' 
```

Now, index a company document:

```
curl "${TYPESENSE_HOST}/collections/companies/documents" \
       -X POST \
       -H "Content-Type: application/json" \
       -H "X-TYPESENSE-API-KEY: ${TYPESENSE_API_KEY}" \
       -d '{
            "id": "124",
            "company_name": "Stark Industries",
            "num_employees": 5215,
            "country": "USA"
          }'
```

Finally, let's search for the document we just indexed:

```
curl -H "X-TYPESENSE-API-KEY: ${API_KEY}" \
     "${TYPESENSE_HOST}/collections/companies/documents/search\
     ?q=stark&query_by=company_name&filter_by=num_employees:>100\
     &sort_by=num_employees:desc"
```

## Building from source

**Building on your machine**

Typesense requires the following dependencies: 

* Snappy
* zlib
* OpenSSL (>=1.0.2)
* curl
* Compiler with good C++11 support (e.g. GCC >= 4.9)

```
$ ./build.sh [--clean] [--depclean]
```

**Building a Docker image**

The Docker build script takes care of the required dependencies, so you don't need to install them separately:

```
$ TYPESENSE_IMG_VERSION=nightly ./docker-build.sh --build-deploy-image [--clean] [--depclean]
```

&copy; 2016-2018 Wreally Studios Inc.