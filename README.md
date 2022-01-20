# freccia
Apache Arrow implementation in Nim

*Early experiments*

#TODO

### Basic:
- [X] implement Arrow C data interface
- [X] read Arrow Scheme from C interface
- [X] read numerical data from C interface
- [X] zero-copy view as openArray
- [X] handle null bitmask
- [X] implement Flatbuffer scheme for metadata
- [X] parse format string into metadata Type

### Layouts:
- [X] handle fixed-size primitive layout
- [ ] handle variable-size binary Layout
- [ ] handle variable-size List layout
- [ ] handle fixed-Size list layout
- [ ] handle struct layout
- [ ] handle dense union layout
- [ ] handle sparse union layout
- [ ] handle null layout
- [ ] handle dictionary-encoded layout
