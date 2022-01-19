import std/[strformat, strutils]

# From https://arrow.apache.org/docs/format/CDataInterface.html


type
  ArrowType* {.size: sizeof(int32).} = enum
    atInvalid = "invalid"
    atNull = "null"
    atBoolean = "bool"
    atInt8 = "int8"
    atUInt8 = "uint8"
    atInt16 = "int16"
    atUInt16 = "uint16"
    atInt32 = "int32"
    atUInt32 = "uint32"
    atInt64 = "int64"
    atUInt64 = "uint64"
    #atFloat16 = "float16"
    atFloat32 = "float32"
    atFloat64 = "float64"
    atBinary = "binary"
    atLargeBinary = "large binary"
    atUTF8String = "utf8 string"
    atLargeUtf8String = "large utf8 string"
    #atDecimal128 = "decimal128"
    #aFixedWidth = "fixed width"
    #atTemporal = "temporal"

  ArrowFlag* {.size: sizeof(int64).} = enum
    afDictionaryOrdered = 1
    afNullable
    afMapKeysSorted

  ArrowMetadata* = object
    nKeys: cint

  ArrowSchema* = object
    format: cstring
    name: cstring
    metadata: ptr UncheckedArray[char]
    flags: set[ArrowFlag]
    nChildren: int64
    children: ptr UncheckedArray[ptr ArrowSchema]
    dictionary: ptr ArrowSchema
    release: proc(a: ptr ArrowSchema): void {.cdecl.}
    privateData: pointer

  ArrowArray* = object
    length: int64
    nullCount: int64
    offset: int64
    nBuffers: int64
    nChildren: int64
    buffers: ptr UncheckedArray[pointer]
    children: ptr UncheckedArray[ptr ArrowArray]
    dictionary: ptr ArrowArray
    release: proc(a: ptr ArrowArray): void {.cdecl.}
    privateData: pointer

  ArrowBaseStructure = ArrowSchema | ArrowArray


# Compile time goodies

const 
  formatTypeMap* = block:
    var res: array[char, ArrowType]
    res['n'] = atNull
    res['b'] = atBoolean
    res['c'] = atInt8
    res['C'] = atUInt8
    res['s'] = atInt16
    res['S'] = atUInt16
    res['i'] = atInt32
    res['I'] = atUInt32
    res['l'] = atInt64
    res['L'] = atUInt64
    res['e'] = atInvalid  # not supported https://github.com/nim-lang/Nim/issues/12769
    res['f'] = atFloat32
    res['g'] = atFloat64
    res['z'] = atBinary
    res['Z'] = atLargeBinary
    res['u'] = atUTF8String
    res['U'] = atLargeUtf8String
    # TODO https://arrow.apache.org/docs/format/CDataInterface.html#c.ArrowSchema.format
    res['d'] = atInvalid
    res['w'] = atInvalid
    res['t'] = atInvalid
    res
  formatTypeMapInv* = block:
    var res: array[ArrowType, char]
    for key, val in formatTypeMap:
      if val != atInvalid:
        res[val] = key
    res
  formatChars* = block:
    var res: set[char]
    for key, val in formatTypeMap:
      if val != atInvalid:
        res.incl key
    res

template dtype*(at: ArrowType): typedesc =
  when at == atInvalid: {.error.}
  elif at == atNull: void
  elif at == atBoolean: bool
  elif at == atInt8: int8
  elif at == atUInt8: uint8
  elif at == atInt16: int16
  elif at == atUInt16: uint16
  elif at == atInt32: int32
  elif at == atUInt32: uint32
  elif at == atInt64: int64
  elif at == atUInt64: uint64
  elif at == atFloat32: float32
  elif at == atFloat64: float64
  # TODO
  elif at == atBinary: {.error.}
  elif at == atLargeBinary: {.error.}
  elif at == atUTF8String: {.error.}
  elif at == atLargeUtf8String: {.error.}
  else: {.error.}

template children*(abs: ArrowBaseStructure): openArray[ptr ArrowBaseStructure] =
  abs.children.toOpenArray(0, abs.nChildren.int-1)

template buffers*(arr: ArrowArray): openArray[pointer] =
  arr.buffers.toOpenArray(0, arr.nBuffers.int-1)

template values*[T](arr: ArrowArray): openArray[T] =
  let 
    validityBitmap = arr.buffers[0]
    values = cast[ptr UncheckedArray[T]](arr.buffers[1])
  values.toOpenArray(0, arr.length.int-1)


# Getters

func format*(sch: ArrowSchema): string =
  $sch.format

func length*(arr: ArrowArray): int64 =
  arr.length

func nullCount*(arr: ArrowArray): int64 =
  arr.nullCount

func parseType*(sch: ArrowSchema): ArrowType =
  let fchar = sch.format[0]
  if fchar in formatChars:
    formatTypeMap[fchar]
  else:
    atInvalid


func size*(at: ArrowType): int =
  template err = raise newException(ValueError, &"Invalid type {$at}")
  case at:
  of atInvalid: err()
  of atNull: atNull.dtype.sizeof
  of atBoolean: atBoolean.dtype.sizeof
  of atInt8: atInt8.dtype.sizeof
  of atUInt8: atUInt8.dtype.sizeof
  of atInt16: atInt16.dtype.sizeof
  of atUInt16: atUInt16.dtype.sizeof
  of atInt32: atInt32.dtype.sizeof
  of atUInt32: atUInt32.dtype.sizeof
  of atInt64: atInt64.dtype.sizeof
  of atUInt64: atUInt64.dtype.sizeof
  of atFloat32: atFloat32.dtype.sizeof
  of atFloat64: atFloat64.dtype.sizeof
  # TODO
  of atBinary: err()
  of atLargeBinary: err()
  of atUTF8String: err()
  of atLargeUtf8String: err()


# Memory management

proc release*(abs: ArrowBaseStructure) = 
  abs.release(abs.unsafeAddr)



# Pretty

func `$`*(abs: ArrowBaseStructure): string =
  result &= $abs.type & "\n"
  
  when abs is ArrowSchema:
    result &= &"format: {abs.format} ({abs.parseType})\n"
    result &= &"name: {abs.name}\n"
    result &= &"metadata.isNil: {abs.metadata.isNil}\n"
    result &= &"flags: {abs.flags}\n"
  
  when abs is ArrowArray:
    result &= &"length: {abs.length}\n"
    result &= &"nullCount: {abs.nullCount}\n"
    result &= &"offset: {abs.offset}\n"
    result &= &"nBuffers: {abs.nBuffers}\n"
    for i, buffer in buffers(abs):
      result &= &"buffer[{i}]: {repr buffer}\n"

  result &= &"nChildren: {abs.nChildren}\n"
  for i, child in children(abs):
    if not child.isNil:
      result &= &"child[{i}]: {child[]}\n"
  result &= &"dictionary.isNil: {abs.dictionary.isNil}\n"
  if not abs.dictionary.isNil:
    result &= &"dictionary: {abs.dictionary[]}\n"
  result &= &"release.isNil: {abs.release.isNil}\n"
  result &= &"privateData.isNil: {abs.privateData.isNil}\n"



# Producers TODO 
proc releaseExported[T: ptr ArrowBaseStructure](bs: T) {.cdecl.} =
  when T is ArrowSchema:
    doAssert not bs.format.isNil

  for i in 0..<bs.nChildren:
    let child: T = bs.children[i]
    if not child.isNil:
      child.release(child)
      doAssert child.release.isNil

  let dict: T = bs.dictionary
  if not dict.isNil and not dict.release.isNil:
    dict.release(dict)
    doAssert dict.release.isNil

  bs.release = nil

proc exportInt32Type*(s: ptr ArrowSchema) =
  s.format = "i".cstring
  s.name = ""
  s.metadata = nil
  s.flags = {}
  s.nChildren = 0
  s.children = nil
  s.dictionary = nil
  s.release = releaseExported