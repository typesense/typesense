# Typesense

Typesense is an open source search engine for building a delightful search experience.

- **Typo tolerance:** Handles typographical errors out-of-the-box
- **Tunable ranking + relevancy:** Tailor your search results to perfection
- **Blazing fast:** Meticulously designed and optimized for speed
- **Simple and delightful:** Dead simple API, delightful out-of-the-box experience

## Quick start

TODO

## Dependencies

* [RocksDB](https://github.com/facebook/rocksdb)
* [libfor](https://github.com/cruppstahl/for/)
* [h2o](https://github.com/h2o/h2o)

## Building Typesense from source

### OS X

Homebrew dependencies:

```
brew install cmake
brew install openssl
brew install h2o
brew install rocksdb
```

Building libfor:

```
cd external/libfor
make
```

Finally, building Typsense:

```
./build.sh
```

&copy; 2016-2017 Wreally Studios Inc.