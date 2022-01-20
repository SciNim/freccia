# From https://arrow.apache.org/docs/format/CDataInterface.html

type
  ArrowFlag* {.size: sizeof(int64).} = enum
    afDictionaryOrdered = 1
    afNullable
    afMapKeysSorted

  ArrowMetadata* = object
    nKeys*: cint

  ArrowSchema* = object
    format*: cstring
    name*: cstring
    metadata*: ptr UncheckedArray[char]
    flags*: set[ArrowFlag]
    nChildren*: int64
    children*: ptr UncheckedArray[ptr ArrowSchema]
    dictionary*: ptr ArrowSchema
    release*: proc(a: ptr ArrowSchema): void {.cdecl.}
    privateData*: pointer

  ArrowArray* = object
    length*: int64
    nullCount*: int64
    offset*: int64
    nBuffers*: int64
    nChildren*: int64
    buffers*: ptr UncheckedArray[pointer]
    children*: ptr UncheckedArray[ptr ArrowArray]
    dictionary*: ptr ArrowArray
    release*: proc(a: ptr ArrowArray): void {.cdecl.}
    privateData*: pointer