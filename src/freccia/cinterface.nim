import std/[strformat, strutils]

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

  
func toString(abs: CBaseStructure): string =
  template append(s: string): untyped = result &= s.indent(2) & "\n"
  append $abs.type
  when abs is CSchema:
    append &"format: {abs.format} ({($abs.format).parseType})"
    append &"name: {abs.name}"
    append &"metadata.isNil: {abs.metadata.isNil}"
    append &"flags: {abs.flags}"
  when abs is CArray:
    append &"length: {abs.length}"
    append &"nullCount: {abs.nullCount}"
    append &"offset: {abs.offset}"
    append &"nBuffers: {abs.nBuffers}"
    for i, buffer in abs.bufferList:
      append &"buffer[{i}]: {repr buffer}"
  append &"dictionary.isNil: {abs.dictionary.isNil}"
  if not abs.dictionary.isNil:
    append &"dictionary: {abs.dictionary[]}"
  append &"release.isNil: {abs.release.isNil}"
  append &"privateData.isNil: {abs.privateData.isNil}"
  append &"nChildren: {abs.nChildren}"
  for i, child in abs.childrenList:
    if not child.isNil:
      append &"child[{i}]: \n{child[]}"


func `$`*(abs: CBaseStructure): string = abs.toString()


func childrenList*(abs: CBaseStructure): openArray[ptr CBaseStructure] =
  abs.children.toOpenArray(0, abs.nChildren.int-1)


func bufferList*(arr: CArray): openArray[pointer] =
  arr.buffers.toOpenArray(0, arr.nBuffers.int-1)


proc rootRelease*(abs: CBaseStructure) = 
  abs.release(abs.unsafeAddr)