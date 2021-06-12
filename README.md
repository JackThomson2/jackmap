# JackMap Concurrent Hashmap

This is a very experimental lockfree hashtable using the *flize* garbage collector.

Base implementation is around the Abseil / Hashbrown hashtables but adapted to try and be lockfree and concurrent. 

Lookup sped up using SIMD instructions, this is run on atomic u8's which offer a mapping to find cell's faster. An [article](https://rigtorp.se/isatomic/) here outlines, despite there being no official spec around SIMD and atomics they tend to support it.