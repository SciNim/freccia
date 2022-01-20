import std/[unittest]
import nimpy
import nimpy/py_lib
import freccia




test "parser":
  template checkstr(a: untyped, b: untyped) = check $a == $b
  # https://forum.nim-lang.org/t/6781
  checkstr parseFormat("d:12,20"), Type(kind: tkDecimal, decimalMeta: Decimal(precision: 12, scale: 20, bitWidth: 128))
  checkstr parseFormat("d:19,10,256"), Type(kind: tkDecimal, decimalMeta: Decimal(precision: 19, scale: 10, bitWidth: 256))
  checkstr parseFormat("w:42"), Type(kind: tkFixedSizeBinary, fixedSizeBinaryMeta: FixedSizeBinary(byteWidth: 42))
  checkstr parseFormat("tdD"), Type(kind: tkDate, dateMeta: Date(unit: duDay))
  checkstr parseFormat("ttn"), Type(kind: tkTime, timeMeta: Time(unit: tuNanosecond, bitWidth: 64))
  checkstr parseFormat("tsu:Europe/Rome"), Type(kind: tkTimestamp, timestampMeta: Timestamp(unit: tuMicrosecond, zone: "Europe/Rome"))
  checkstr parseFormat("tsu:"), Type(kind: tkTimestamp, timestampMeta: Timestamp(unit: tuMicrosecond, zone: ""))
  checkstr parseFormat("tDs"), Type(kind: tkDuration, durationMeta: Duration(unit: tuSecond))
  checkstr parseFormat("tiD"), Type(kind: tkInterval, intervalMeta: Interval(unit: iuDayTime))
  checkstr parseFormat("+l"), Type(kind: tkList, listMeta: List())
  checkstr parseFormat("+L"), Type(kind: tkLargeList, largeListMeta: LargeList())
  checkstr parseFormat("+w:418"), Type(kind: tkFixedSizeList, fixedSizeListMeta: FixedSizeList(listSize: 418))
  checkstr parseFormat("+ud:123,2,31,4000,5123"), Type(kind: tkUnion, unionMeta: Union(mode: umDense, typeIds: @[123.int32, 2, 31, 4000, 5123]))



test "format":
  block:
    let fmtType = formatTypeMap['I']
    check fmtType == atUInt32
    check fmtType.size == 4
  block:
    let fmtType = formatTypeMap['n']
    check fmtType == atNull
    check fmtType.size == 0
  block:
    let fmtType = formatTypeMap['g']
    check fmtType == atFloat64
    check fmtType.size == 8
  block:
    for c in ['a','e','d','w','t','Q']:
      let fmtType = formatTypeMap[c]
      check fmtType == atInvalid
      expect ValueError:
        discard fmtType.size
  block:
    check not compiles(atInvalid.dtype)
    check not compiles(atBinary.dtype)
    check not compiles(atLargeBinary.dtype)
    check not compiles(atUTF8String.dtype)
    check not compiles(atLargeUtf8String.dtype)



# https://github.com/apache/arrow/blob/97879eb970bac52d93d2247200b9ca7acf6f3f93/python/pyarrow/tests/test_cffi.py#L109
# https://github.com/apache/arrow/blob/488f084280fa5e2acea76dcb02dd0c3ee655f55b/python/pyarrow/array.pxi#L1312
test "pyarrow to nim stack":
  pyInitLibPath("/home/jack/.pyenv/versions/3.10.1/lib/libpython3.10.so.1.0")
  var
    py = pyBuiltinsModule()
    pa = pyImport("pyarrow")
    pyears = py.list()
    sch: ArrowSchema
    arr: ArrowArray
  discard pyears.append 1995
  discard pyears.append 1996
  discard pyears.append 1997
  discard pyears.append py.None
  discard pyears.append 1998
  discard pyears.append py.None
  discard pyears.append 2000
  var years = pa.`array`(pyears, type=pa.callMethod("uint32"))
  discard years.callMethod("_export_to_c", cast[int](arr.addr), cast[int](sch.addr))

  const at = atUInt32
  template dt = atType.dtype
  check sch.format[0] == formatTypeMapInv[at]
  check sch.parseType == at
  check arr.length == 7
  check arr.nullCount == 2
  check arr.bufferList.len == 2
  check arr.childrenList.len == 0
  #echo $sch
  #echo $arr
  for i, v in arr.values[: at.dtype]:
    let pyObj = pyears[i]
    if pyObj == py.None:
      check v == 0 # not strictly required
      check not arr.isValid(i)
    else:
      check arr.isValid(i)
      check pyObj.to(at.dtype) == v



# producers
test "do the stack":
  var s = ArrowSchema()
  exportInt32Type(s.addr)
  s.rootRelease()



test "do the heap":
  var s: ptr ArrowSchema = cast[ptr ArrowSchema](alloc(ArrowSchema.sizeof))
  exportInt32Type(s)
  s[].rootRelease()
  dealloc(s)