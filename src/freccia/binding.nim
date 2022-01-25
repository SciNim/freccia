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
    carray*: CArray
    typeinfo: Type
    # children: seq[ArrowArray]
    offset: int
    stride: int
    len: int


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
  result = ArrowArray(
    cschema: cschema, 
    carray: carray, 
    typeinfo: typeinfo,
    # View on data
    offset: 0,
    stride: 1,
    len: carray.length.int
  )
    

# func viewLow(arr: ArrowArray): int = arr.offset
# func viewHigh(arr: ArrowArray): int = (arr.offset + arr.len) - 1
func len*(arr: ArrowArray): int = arr.len
func low*(arr: ArrowArray): int = 0
func high*(arr: ArrowArray): int = arr.len - 1
func `$`*(arr: ArrowArray): string = $arr.cschema & "\n\n" & $arr.carray


proc slice*(arr: ArrowArray, start, stop, step: int): ArrowArray =
  ArrowArray(
    cschema: arr.cschema, 
    carray: arr.carray, 
    typeinfo: arr.typeinfo,
    offset: arr.offset + start * arr.stride,
    stride: arr.stride * step,
    len: min(((stop-start) div step) + 1, arr.carray.length.int)
  )


# --------------------------------------------------------------


template toType*(T: typedesc): Type =
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


func layout*(dtype: Type): LayoutKind =
  case dtype.kind:
  of fixedSizeTypes: alPrimitive
  of variableSizeTypes: alVariableBinary
  of listTypes: alVariableList
  of tkFixedSizeList: alFixedList
  of tkStruct: alStruct
  of tkUnion: 
    case dtype.unionMeta.mode:
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
  if arr.layout in validableLayouts: result = view 0
  else: raise newException(ValueError, "Layout has no validity buffer")


func offsetsBuffer*[T: int32 | int64](arr: ArrowArray): openArray[T] =
  template view(i: int): untyped = 
    # The offsets buffer contains length + 1 signed integers
    cast[ptr UncheckedArray[T]](arr.carray.bufferList[i]).toOpenArray(0, arr.carray.length.int)
  if arr.layout in offsettableLayouts: result = view 1
  else: raise newException(ValueError, "Layout has no offsets buffer")


func dataBuffer*[T](arr: ArrowArray): openArray[T] =
  template err = raise newException(ValueError, "Layout has no data buffer")
  template view(i: int, high: int): untyped = 
    cast[ptr UncheckedArray[T]](arr.carray.bufferList[i]).toOpenArray(0, high)
  case arr.layout:
  of alPrimitive: result = view(1, arr.carray.length.int-1)
  of alVariableBinary: 
    case arr.typeinfo.kind
    of tkBinary, tkUtf8: result = view(2, arr.offsetsBuffer[:int32][^1].int)
    of tkLargeBinary, tkLargeUtf8: result = view(2, arr.offsetsBuffer[:int64][^1].int)
    else: err()
  else: err()


# --------------------------------------------------------------


func getItem[T](arr: ArrowArray, i: int): T =

  func offsetView(arr: ArrowArray, T: typedesc, i: int): openArray[byte] =
    let
      offsets = arr.offsetsBuffer[:T]
      slotStart = offsets[i]
      slotEnd = offsets[i+1] - 1
    result = arr.dataBuffer[:byte].toOpenArray(slotStart.int, slotEnd.int)

  template err = raise newException(ValueError, "Unable to get item")

  when T is openArray[byte]:
    if arr.layout == alVariableBinary:
      case arr.typeinfo.kind
      of tkBinary, tkUtf8: result = arr.offsetView(int32, i)
      of tkLargeBinary, tkLargeUtf8: result = arr.offsetView(int64, i)
      else: err()
    else: err()
  else:
    if arr.layout == alPrimitive:
      result = arr.dataBuffer[:T][i]
    else: err()


func item*[T](arr: ArrowArray, i: int): T = arr.getItem[:T](arr.offset + i * arr.stride)
func item*(arr: ArrowArray, i: int, T: typedesc): T = arr.getItem[:T](arr.offset + i * arr.stride)


# iterator items*[T](arr: ArrowArray): T = 
#   for i in arr.viewLow..arr.viewHigh: yield arr.getItem[:T](i)
# iterator items*(arr: ArrowArray, T: typedesc): T = 
#   for i in arr.viewLow..arr.viewHigh: yield arr.getItem[:T](i)

iterator items*[T](arr: ArrowArray): T =
  var cur = arr.offset
  for _ in 0 ..< arr.len:
    yield arr.item(cur, T)
    cur += arr.stride


func isValid*(arr: ArrowArray, i: int): bool =
  if arr.carray.nullCount == 0: result = true
  else: 
    let j = arr.offset + i * arr.stride
    result = (arr.validityBuffer[j div 8] and (1.byte shl (j mod 8))).bool

# --------------------------------------------------------------


# func asPrimitiveView*[T](arr: ArrowArray): View[T] {.inline.} =
#   assert arr.layout == alPrimitive
#   arr.dataBuffer[T].toView


# func asVariableBinaryView*(arr: ArrowArray): View[View[byte]] {.inline.} =
#   assert arr.layout == alVariableBinary
#   let viewSeq = collect(newSeqOfCap(arr.carray.length)):
#       for item in arr.items(openArray[byte]):
#         item.toView
#   result = viewSeq.toOpenArray.toView

  