# freccia
Apache Arrow implementation in Nim

*Early experiments*

# TODO

### Producer:
- [ ] generation of ArrowArray

### Consumer:
- [X] implement Arrow C data interface
- [X] read ArrowScheme from C interface
- [X] read numerical data from C ArrowArray
- [X] zero-copy view as openArray
- [X] handle null bitmask
- [X] implement Flatbuffer scheme for metadata
- [X] parse format string into type + metadata
- [X] slice/stride view
- [X] slice/stride iterator

### Layouts:
- [X] handle fixed-size primitive layout
- [X] handle variable-size binary Layout
- [X] handle variable-size List layout
- [ ] handle fixed-size list layout
- [ ] handle struct layout
- [ ] handle dense union layout
- [ ] handle sparse union layout
- [ ] handle null layout
- [ ] handle dictionary-encoded layout

### Types:
- [X] Null
- [X] Int
- [X] FloatingPoint
- [X] Binary
- [!] LargeBinary
- [X] Utf8
- [!] LargeUtf8
- [X] Bool
- [X] List
- [!] LargeList
- [ ] FixedSizeList
- [ ] FixedSizeBinary
- [ ] Decimal
- [ ] Date
- [ ] Time
- [ ] Timestamp
- [ ] Interval
- [ ] Duration
- [ ] Struct
- [ ] Union
- [ ] Map