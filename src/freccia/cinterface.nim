{.experimental: "views".}


import std/[strformat]

# From https://arrow.apache.org/docs/format/CDataInterface.html

type
  CFlag* {.size: sizeof(int64).} = enum
    cfDictionaryOrdered = 1
    cfNullable
    cfMapKeysSorted

  CMetadata* = object
    nKeys*: cint

  CSchema* = object
    format*: cstring
    name*: cstring
    metadata*: ptr UncheckedArray[char]
    flags*: set[CFlag]
    nChildren*: int64
    children*: ptr UncheckedArray[ptr CSchema]
    dictionary*: ptr CSchema
    release*: proc(a: ptr CSchema): void {.cdecl.}
    privateData*: pointer

  CArray* = object
    length*: int64
    nullCount*: int64
    offset*: int64
    nBuffers*: int64
    nChildren*: int64
    buffers*: ptr UncheckedArray[pointer]
    children*: ptr UncheckedArray[ptr CArray]
    dictionary*: ptr CArray
    release*: proc(a: ptr CArray): void {.cdecl.}
    privateData*: pointer

  CBaseStructure* = CSchema | CArray



func `$`*(abs: CBaseStructure): string =
  result &= $abs.type & "\n"
  when abs is CSchema:
    result &= &"format: {abs.format} ({($abs.format).parseType})\n"
    result &= &"name: {abs.name}\n"
    result &= &"metadata.isNil: {abs.metadata.isNil}\n"
    result &= &"flags: {abs.flags}\n"
  when abs is CArray:
    result &= &"length: {abs.length}\n"
    result &= &"nullCount: {abs.nullCount}\n"
    result &= &"offset: {abs.offset}\n"
    result &= &"nBuffers: {abs.nBuffers}\n"
    for i, buffer in abs.bufferList:
      result &= &"buffer[{i}]: {repr buffer}\n"
  result &= &"nChildren: {abs.nChildren}\n"
  for i, child in abs.childrenList:
    if not child.isNil:
      result &= &"child[{i}]: {child[]}\n"
  result &= &"dictionary.isNil: {abs.dictionary.isNil}\n"
  if not abs.dictionary.isNil:
    result &= &"dictionary: {abs.dictionary[]}\n"
  result &= &"release.isNil: {abs.release.isNil}\n"
  result &= &"privateData.isNil: {abs.privateData.isNil}\n"


func childrenList*(abs: CBaseStructure): openArray[ptr CBaseStructure] =
  abs.children.toOpenArray(0, abs.nChildren.int-1)


func bufferList*(arr: CArray): openArray[pointer] =
  arr.buffers.toOpenArray(0, arr.nBuffers.int-1)


proc rootRelease*(abs: CBaseStructure) = 
  abs.release(abs.unsafeAddr)