import std/[sugar]
import cinterface
import schema
import parser


type
  # https://arrow.apache.org/docs/format/Columnar.html#id2
  # Offset = int32 | int64
  # Buffer[T] = ptr UncheckedArray[T]
  # ValidityBuffer = Buffer[byte]
  # OffsetBuffer[T: Offset] = Buffer[T]
  # TypeIdBuffer = Buffer[int8]
  # PrimitiveLayout[T] = object
  #   validity: ValidityBuffer
  #   data: Buffer[T]
  # VariableBinaryLayout[B: Offset] = object
  #   validity: ValidityBuffer
  #   offsets: OffsetBuffer[B]
  #   data: Buffer[byte]
  # VariableListLayout[B: Offset] = object
  #   validity: ValidityBuffer
  #   offsets: OffsetBuffer[B]
  # FixedSizeListLayout[T, size: static[int32]] = object
  #   validity: ValidityBuffer
  # StructLayout[T: tuple] = object
  #   validity: ValidityBuffer
  # UnionSparseLayout[T: tuple] = object
  #   typeIds: TypeIdBuffer
  # UnionDenseLayout[T: tuple] = object
  #   typeIds: TypeIdBuffer
  #   offsets: OffsetBuffer[int32]
  # NullLayout = object
  # DictionaryLayout[T: SomeSignedInt] = object
  #   validity: ValidityBuffer
  #   indices: Buffer[T]
  # Layout = PrimitiveLayout | VariableBinaryLayout | VariableListLayout | FixedSizeListLayout |
  #   StructLayout | UnionSparseLayout | UnionDenseLayout | NullLayout | DictionaryLayout
  # AbstractType = Null | Int | FloatingPoint | Binary | Utf8 | Bool | Decimal | Date | Time | Timestamp | Interval |
  #   List | Struct | Union | FixedSizeBinary | FixedSizeList | Map | Duration | LargeBinary | LargeUtf8 | LargeList
  # ConcreteType = Null | int16 | int32 | int64 | uint16 | uint32 | uint64 | float32 | float64 | bool
  LayoutKind* = enum
    alPrimitive = "primitive (fixed-size)"
    alVariableBinary = "variable-size binary"
    alVariableList = "variable-size list"
    alFixedList = "fixed-size list"
    alStruct = "struct"
    alUnionSparse = "sparse union"
    alUnionDense = "dense union"
    alNull = "null sequence"
    alDictionary = "dictionary encoded"
  ArrowArray* = object
    cschema: CSchema
    carray: CArray
    typeinfo: Type
    children: seq[ArrowArray]


const 
  fixedSizeTypes = {tkInt, tkFloatingPoint, tkBool, tkDecimal, tkFixedSizeBinary, tkDate, tkTime, tkTimestamp, tkInterval, tkDuration}
  variableSizeTypes = {tkBinary, tkLargeBinary, tkUtf8, tkLargeUtf8}
  listTypes = {tkList, tkLargeList}
  validableLayouts = {alPrimitive, alVariableBinary, alVariableList, alFixedList, alStruct, alDictionary}
  offsettableLayouts = {alVariableBinary, alVariableList, alUnionDense}
  
  
# --------------------------------------------------------------


proc initArrowArray*(cschema: CSchema, carray: CArray): ArrowArray =
  assert cschema.nChildren == carray.nChildren
  let typeinfo = ($cschema.format).parseType
  let children = collect(newSeqOfCap(cschema.nChildren)):
    for i in 0..<cschema.nChildren.int:
      initArrowArray(cschema.children[i][], carray.children[i][])
  result = ArrowArray(cschema: cschema, carray: carray, typeinfo: typeinfo, children: children)
    


proc `$`*(arr: ArrowArray): string =
  $arr.cschema & "\n\n" & $arr.carray



# --------------------------------------------------------------


template toType*[T](_: typedesc[T]): Type =
  when T is int16: Type(kind:tkInt, intMeta: Int(bitWidth:16, isSigned: true))
  elif T is int32: Type(kind:tkInt, intMeta: Int(bitWidth:32, isSigned: true))
  elif T is int64: Type(kind:tkInt, intMeta: Int(bitWidth:64, isSigned: true))
  elif T is uint16: Type(kind:tkInt, intMeta: Int(bitWidth:16, isSigned: false))
  elif T is uint32: Type(kind:tkInt, intMeta: Int(bitWidth:32, isSigned: false))
  elif T is uint64: Type(kind:tkInt, intMeta: Int(bitWidth:64, isSigned: false))
  elif T is float32: Type(kind:tkFloatingPoint, floatingPointMeta: FloatingPoint(precision:pSingle))
  elif T is float64: Type(kind:tkFloatingPoint, floatingPointMeta: FloatingPoint(precision:pDouble))
  elif T is string: Type(kind: tkUtf8, utf8Meta: Utf8())
  else: {.error.}


func `==`*(a: Type, b: Type): bool =
  a.kind == b.kind and (case a.kind:
    of tkNull: a.nullMeta == b.nullMeta
    of tkInt: a.intMeta == b.intMeta
    of tkFloatingPoint: a.floatingPointMeta == b.floatingPointMeta
    of tkBinary: a.binaryMeta == b.binaryMeta
    of tkUtf8: a.utf8Meta == b.utf8Meta
    of tkBool: a.boolMeta == b.boolMeta
    of tkDecimal: a.decimalMeta == b.decimalMeta
    of tkDate: a.dateMeta == b.dateMeta
    of tkTime: a.timeMeta == b.timeMeta
    of tkTimestamp: a.timestampMeta == b.timestampMeta
    of tkInterval: a.intervalMeta == b.intervalMeta
    of tkList: a.listMeta == b.listMeta
    of tkStruct: a.structMeta == b.structMeta
    of tkUnion: a.unionMeta == b.unionMeta
    of tkFixedSizeBinary: a.fixedSizeBinaryMeta == b.fixedSizeBinaryMeta
    of tkFixedSizeList: a.fixedSizeListMeta == b.fixedSizeListMeta
    of tkMap: a.mapMeta == b.mapMeta
    of tkDuration: a.durationMeta == b.durationMeta
    of tkLargeBinary: a.largeBinaryMeta == b.largeBinaryMeta
    of tkLargeUtf8: a.largeUtf8Meta == b.largeUtf8Meta
    of tkLargeList: a.largeListMeta == b.largeListMeta
  )


func layout*(t: Type): LayoutKind =
  case t.kind:
  of fixedSizeTypes: alPrimitive
  of variableSizeTypes: alVariableBinary
  of listTypes: alVariableList
  of tkFixedSizeList: alFixedList
  of tkStruct: alStruct
  of tkUnion: 
    case t.unionMeta.mode:
      of umSparse: alUnionSparse
      of umDense: alUnionDense
  of tkNull: alNull
  of tkMap: alDictionary


func layout*(arr: ArrowArray): LayoutKind =
  arr.typeinfo.layout


# --------------------------------------------------------------


func validityBuffer*(arr: ArrowArray): openArray[byte] =
  template view(i: int): untyped = 
    cast[ptr UncheckedArray[byte]](arr.carray.bufferList[i]).toOpenArray(0, arr.carray.length.int-1)
  case arr.layout:
  of validableLayouts: result = view 0
  else: raise newException(ValueError, "Layout has no validity buffer")


func offsetsBuffer*[T: int32 | int64](arr: ArrowArray): openArray[T] =
  template view(i: int): untyped = 
    # The offsets buffer contains length + 1 signed integers
    cast[ptr UncheckedArray[T]](arr.carray.bufferList[i]).toOpenArray(0, arr.carray.length.int)
  case arr.layout:
  of offsettableLayouts: result = view 1
  else: raise newException(ValueError, "Layout has no offsets buffer")


func dataBuffer*[T](arr: ArrowArray): openArray[T] =
  template err = raise newException(ValueError, "Layout has no data buffer")
  template view(i: int, len: int): untyped = 
    cast[ptr UncheckedArray[T]](arr.carray.bufferList[i]).toOpenArray(0, len)
  case arr.layout:
  of alPrimitive: result = view(1, arr.carray.length.int-1)
  of alVariableBinary: 
    case arr.typeinfo.kind
    of tkBinary, tkUtf8: result = view(2, arr.offsetsBuffer[:int32][^1].int)
    of tkLargeBinary, tkLargeUtf8: result = view(2, arr.offsetsBuffer[:int64][^1].int)
    else: err()
  else: err()


# --------------------------------------------------------------


func isValid*(arr: ArrowArray, i: int): bool =
  if arr.carray.nullCount == 0: true
  else: (arr.validityBuffer[i div 8] and (1.byte shl (i mod 8))).bool


func item*[T](arr: ArrowArray, i: int): T =
  case arr.layout
  of alPrimitive: arr.dataBuffer[: T][i]
  else: raise newException(ValueError, "Unable to get item")


func item*[T](arr: ArrowArray, i: int, typ: typedesc[T]): T =
  arr.item[: T](i)


func items*[T](arr: ArrowArray): openArray[T] =
  arr.dataBuffer[:T]


# openArray[openArray[byte]] not possible?
func blob*(arr: ArrowArray, i: int): openArray[byte] =
  template view[T](offsetType: typedesc[T]): untyped =
    let
      offsets = arr.offsetsBuffer[:offsetType]
      slotStart = offsets[i]
      slotEnd = offsets[i+1] - 1
    result = arr.dataBuffer[:byte].toOpenArray(slotStart.int, slotEnd.int)
  case arr.typeinfo.kind
  of tkBinary, tkUtf8: view(int32)
  of tkLargeBinary, tkLargeUtf8: view(int64)
  else: raise newException(ValueError, "Unable to get item")


iterator blobs*(arr: ArrowArray): openArray[byte] =
  for i in 0..<arr.carray.length.int:
    yield arr.blob(i)


func child*(arr: ArrowArray, i: int): ArrowArray =
  arr.children[i]


# https://github.com/nim-lang/Nim/issues/19435
# func children*(arr: ArrowArray): openArray[ArrowArray] =
#   arr.children.toOpenArray(0, arr.children.len-1)
iterator children*(arr: ArrowArray): ArrowArray =
  for child in arr.children:
    yield child


# --------------------------------------------------------------



