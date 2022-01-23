import std/[unittest]
import nimpy
import nimpy/py_lib
import freccia



pyInitLibPath("/home/jack/.pyenv/versions/3.10.1/lib/libpython3.10.so.1.0")

var
  py = pyBuiltinsModule()
  pa = pyImport("pyarrow")


proc genNumArray[T](_: typedesc[T]): (PyObject, ArrowArray) =
  var
    pyears = py.list()
    cschema: CSchema
    carray: CArray
  discard pyears.append 1995
  discard pyears.append 1996
  discard pyears.append 1997
  discard pyears.append py.None
  discard pyears.append 1998
  discard pyears.append py.None
  discard pyears.append 2000
  var years = pa.`array`(pyears, type=pa.callMethod($T))
  discard years.callMethod("_export_to_c", cast[int](carray.addr), cast[int](cschema.addr))
  let rt =
    when T is int16: Type(kind:tkInt, intMeta: Int(bitWidth:16, isSigned: true))
    elif T is int32: Type(kind:tkInt, intMeta: Int(bitWidth:32, isSigned: true))
    elif T is int64: Type(kind:tkInt, intMeta: Int(bitWidth:64, isSigned: true))
    elif T is uint16: Type(kind:tkInt, intMeta: Int(bitWidth:16, isSigned: false))
    elif T is uint32: Type(kind:tkInt, intMeta: Int(bitWidth:32, isSigned: false))
    elif T is uint64: Type(kind:tkInt, intMeta: Int(bitWidth:64, isSigned: false))
    elif T is float32: Type(kind:tkFloatingPoint, floatingPointMeta: FloatingPoint(precision:pSingle))
    elif T is float64: Type(kind:tkFloatingPoint, floatingPointMeta: FloatingPoint(precision:pDouble))
    else: {.error.}
  check parseType($cschema.format) == rt
  check carray.length == 7
  check carray.nullCount == 2
  check carray.bufferList.len == 2
  check carray.childrenList.len == 0
  (pyears, initArrowArray(cschema, carray))


proc genBinaryArray: (PyObject, ArrowArray) =
  var
    pyears = py.list()
    cschema: CSchema
    carray: CArray
  discard pyears.append "monkey"
  discard pyears.append py.None
  discard pyears.append "elephant"
  discard pyears.append py.None
  discard pyears.append "dolphin"
  discard pyears.append py.None
  discard pyears.append "tarantula"
  var years = pa.`array`(pyears)
  discard years.callMethod("_export_to_c", cast[int](carray.addr), cast[int](cschema.addr))
  (pyears, initArrowArray(cschema, carray))



test "parser":
  check parseType("d:12,20") == Type(kind: tkDecimal, decimalMeta: Decimal(precision: 12, scale: 20, bitWidth: 128))
  check parseType("d:19,10,256") == Type(kind: tkDecimal, decimalMeta: Decimal(precision: 19, scale: 10, bitWidth: 256))
  check parseType("w:42") == Type(kind: tkFixedSizeBinary, fixedSizeBinaryMeta: FixedSizeBinary(byteWidth: 42))
  check parseType("tdD") == Type(kind: tkDate, dateMeta: Date(unit: duDay))
  check parseType("ttn") == Type(kind: tkTime, timeMeta: Time(unit: tuNanosecond, bitWidth: 64))
  check parseType("tsu:Europe/Rome") == Type(kind: tkTimestamp, timestampMeta: Timestamp(unit: tuMicrosecond, zone: "Europe/Rome"))
  check parseType("tsu:") == Type(kind: tkTimestamp, timestampMeta: Timestamp(unit: tuMicrosecond, zone: ""))
  check parseType("tDs") == Type(kind: tkDuration, durationMeta: Duration(unit: tuSecond))
  check parseType("tiD") == Type(kind: tkInterval, intervalMeta: Interval(unit: iuDayTime))
  check parseType("+l") == Type(kind: tkList, listMeta: List())
  check parseType("+L") == Type(kind: tkLargeList, largeListMeta: LargeList())
  check parseType("+w:418") == Type(kind: tkFixedSizeList, fixedSizeListMeta: FixedSizeList(listSize: 418))
  check parseType("+ud:123,2,31,4000,5123") == Type(kind: tkUnion, unionMeta: Union(mode: umDense, typeIds: @[123.int32, 2, 31, 4000, 5123]))



# https://github.com/apache/arrow/blob/97879eb970bac52d93d2247200b9ca7acf6f3f93/python/pyarrow/tests/test_cffi.py#L109
# https://github.com/apache/arrow/blob/488f084280fa5e2acea76dcb02dd0c3ee655f55b/python/pyarrow/array.pxi#L1312
template genTestType[T](typ:typedesc[T]): untyped =
  test "Primitive " & $typ:
    let (pylist, aarray) = genNumArray(typ)
    check aarray.layout == alPrimitive
    for i, v in aarray.items[:typ]:
      var pyObj = pylist[i]
      if pyObj == py.None:
        check v == 0 # not strictly required
        check not aarray.isValid(i)
      else:
        let pv = pyObj.to(typ)
        check aarray.isValid(i)
        check pv == v # https://github.com/nim-lang/Nim/issues/19426
        check pv == aarray.item[:typ](i)
        check pv == aarray.item(i, typ)
genTestType(int16)
genTestType(int32)
genTestType(int64)
genTestType(uint16)
genTestType(uint32)
genTestType(uint64)
genTestType(float32)
genTestType(float64)


test "Variable binary":
  let (pylist, aarray) = genBinaryArray()
  echo pylist
  echo aarray
  for i in aarray.itemBlob(0):
    echo char(i)
  #let foo: aarray.typeinfo.kind.offsetBitWidth
  #let offsets = cast[ptr UncheckedArray[aarray.typeinfo.kind.offsetBitWidth]](aarray.carray.bufferList[1])
  #for i in 0..aarray.carray.length:
  #  echo offsets[i]


# producers
test "do the stack":
  var s = CSchema()
  exportInt32Type(s.addr)
  s.rootRelease()



test "do the heap":
  var s: ptr CSchema = cast[ptr CSchema](alloc(CSchema.sizeof))
  exportInt32Type(s)
  s[].rootRelease()
  dealloc(s)