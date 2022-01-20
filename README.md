# freccia
Apache Arrow implementation in Nim

*Early experiments*

# TODO

### Basic:
- [X] implement Arrow C data interface
- [X] read ArrowScheme from C interface
- [X] read numerical data from C ArrowArray
- [X] zero-copy view as openArray
- [X] handle null bitmask
- [X] implement Flatbuffer scheme for metadata
- [X] parse format string into type + metadata

### Layouts:
- [X] handle fixed-size primitive layout
- [ ] handle variable-size binary Layout
- [ ] handle variable-size List layout
- [ ] handle fixed-size list layout
- [ ] handle struct layout
- [ ] handle dense union layout
- [ ] handle sparse union layout
- [ ] handle null layout
- [ ] handle dictionary-encoded layout
