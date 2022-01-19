# freccia
Apache Arrow implementation in Nim

*Early experiments*

TODO:
- [X] read basic Arrow Scheme
- [X] read numerical data from ArrowArray as openArray
- [X] handle null bitmask
- [ ] handle binary / large binary types
- [ ] handle UTF8 string / large UTF8 string types
- [ ] handle decimal128 + bitwidth types
- [ ] handle fixed-width binary
- [ ] handle temporal data
- [ ] handle dictionary encoded types
- [ ] handle nested types

Handling all possible buffer physical layouts requires generation of code for handling Flatbuffers.
Experimenting Flatbuffer compiler for Nim here: https://github.com/arkanoid87/patella