import std/[unittest, sugar, random, enumerate, strformat]
import nimpy
import nimpy/py_lib
import freccia


type TestTuple = tuple[pylist: PyObject, aarray: ArrowArray]

randomize(987)


# ------------------------------------------------------------------


converter toString(bytes: openArray[byte]): string =
  if bytes.len > 0:
    result = newString(bytes.len)
    copyMem(result[0].addr, bytes[0].unsafeAddr, bytes.len)
  else:
    result = ""


# ------------------------------------------------------------------


# Tests rely on pyarrow
pyInitLibPath("/home/jack/.pyenv/versions/3.10.1/lib/libpython3.10.so.1.0")
let
  py = pyBuiltinsModule()
  pa = pyImport("pyarrow")
  pyrandom = pyImport("random")



func expectedBufferCount(l: LayoutKind): int =
  case l
    of alNull: 0
    of alFixedList, alStruct, alUnionSparse: 1
    of alPrimitive, alVariableList, alUnionDense, alDictionary: 2
    of alVariableBinary: 3


func expectedChildrenCount(l: LayoutKind): int =
  case l
    of alVariableList: 1
    else: 0


proc check(carray: CArray, logicalType: Type, size: int, nNulls: int) =
  check carray.length == size
  check carray.nullCount == nNulls
  check carray.bufferList.len == logicalType.layout.expectedBufferCount
  check carray.childrenList.len == logicalType.layout.expectedChildrenCount


proc check[T,J](arr: ArrowArray, pylist: PyObject) =
  let pylen = py.callMethod("len", pylist).to(int)
  check arr.len == pylen
  check arr.low == 0
  check arr.high == pylen-1

  case arr.logicalType.kind
  of tkList, tkLargeList: # checking recursively
    for i in 0..<pylen:
      let  pyItem = pylist[i]
      if pyItem == py.None:
        check not arr.isValid(i)
      else:
        check arr.isValid(i)
        check[T,J](arr.item(i, ArrowArray), pyItem)
  else:
    for i in 0..<pylen:
      let 
        pyItem = pylist[i]
        aItem = arr.item(i, T)
      if pyItem == py.None:
        check not arr.isValid(i)
      else:
        check arr.isValid(i)
        check aItem == pyItem.to(J) # https://github.com/nim-lang/Nim/issues/19426
    for i, aVal in enumerate arr.items(T):
      if arr.isValid(i):
        check aVal == pylist[i].to(J)


# ----------------------------------------------------------------------


proc genArrowArray[T](size: int, nullsRatio: float, generator: proc(i: int): T, pyarrowType: PyObject): TestTuple =
  var
    cschema: CSchema
    carray: CArray
    nNulls = (size.float*nullsRatio).int
    nValues = size - nNulls
    pylist = py.list()

  for i in 0..<nValues:
    discard pylist.append generator(i)
  for i in 0..<nNulls:
    discard pylist.append py.None
  discard pyrandom.callMethod("shuffle", pylist)

  var pyarray = pa.`array`(pylist, type=pyarrowType)
  discard pyarray.callMethod("_export_to_c", cast[int](carray.addr), cast[int](cschema.addr))
  let logicalType = parseType($cschema.format)
  carray.check(logicalType, size, nNulls)
  (pylist, initArrowArray(cschema, carray))


proc genPrimitiveArray(T: typedesc, size: int, nullsRatio: float): TestTuple =
  proc generator(i: int): T = i.T
  genArrowArray(size, nullsRatio, generator, pa.callMethod($T))


proc genBinaryArray(size: int, nullsRatio: float): TestTuple =
  const strSet = [
    "monkey", "elephant", "dolphin", "tarantula", "Supercalifragilisticexpialidocious", 
    "S̶̢̰̟̙̯͎̰͓̉̾̌͌̔̓͋̎̆͛̅͐͊͜u̴̮̦͇̼͙̦̗͑̐͐̏ͅͅp̴̨̝̮̙̔̚͠ê̸͕̥̿̎͋͑̍̿͝ṙ̴̪̳̮̠͇̱̠̀̎́̓̊̀̀̾̀̓̌̿̚͜ͅc̸̤̗̟͊̏̉͜a̶̧̨̟̗̠̟͖͕͕̣̺͇̙̤͊̑͝ͅl̴̛̫̒̂̆̃͗̐͑̄̍̓́͂̀̚i̴̛͖̾̋͑̎̾̈̏͌̒̿̚̚f̶̡̢͎̳͚̙̣̼͖̱̪̺̬́̀̊͋̉͌̇͋͒̄̿̈́͑̔r̴̨̛̛̗̟̙̅͐̿̓͋ǎ̷̙̰̜̭̠̼͑̋̇̌̐̈́͊̄͛̾͠g̸̢̧̯̗͉̘͎͔̉͒́̚i̸̥͍͈̗͍͍͎̘͌̔͊́̎̈́͋̆̕̕l̶̨̛̯̻̯̺̫̊̄̋̀̿̾̄̿̋ĩ̵̪̰̜̺̖̭͕̄̇͊̑̏̄̀̀̑̈́̍̋̚͝ͅs̵̛͖͚̼̀̂̐͐̚t̸̰̋̃̌̀̿̑͌ì̶̘͇͕̬̫̣̼͆̂̂͛͒̚c̶̢̛̲͉͕̮̩̗̼̱̭̘̳̈̑̌̓͝ę̷̪̰͇̬̿̿̉̃̾͜͠x̷̨̟̱͍̩͈̝̆͊́̑͐̕͜͝͝p̵̧̪̭̥͕̘̟̠̽̆̇̑̌̏̏͆͊̈́̈́̒͂̕͝i̶̧̨̦͖̟̼̮̠̠̤̬̺̙̜̓a̵̯̟̫̣͔̯̭͎̪͚̗̗̣͔͚͐̀͋̆͗̔͂̀̈͘l̶̤͎̞̓̂̊͑ͅḭ̷̫̻̖͚͓̦̻̦̱̭̑ͅd̶͈̾̂̎ò̷̧̢͈͖̹̹̭͓̲̐c̷̢̡̬͖̦̳̹̥͇̲̺͊͌͊̓̀̂̓̆̅ͅị̴̢͙̦̜̟̹̹̮́̀̎͑͒̆̈̎̀̀̂̉̕͜͝͝ơ̸̭̘̫̖̹͕͕̯̑͂̆͊̀́̇̋̋̇̈́̎͘͠ů̶̢̨̜̻̞̟̤̓̑s̶̨͇̦̫͙̞̥̽̊̋͗͋͛͛̈̈́̚̚͘", "丂ひｱ乇尺ᄃﾑﾚﾉｷ尺ﾑムﾉﾚﾉ丂ｲﾉᄃ乇ﾒｱﾉﾑﾚﾉりのᄃﾉのひ丂", 
    "👺☯  Ⓢ𝕦卩ⓔŘc𝐀𝕃ƗⒻℝ卂𝔾Ⓘ𝐋ⒾＳᵗ丨c𝑒𝕩𝔭Ꭵ𝐀Ĺ𝒾𝒹Ỗ𝒸丨𝐨𝔲ᔕ  👽☮"]
  proc generator(i: int): string = $i & "_" & strSet.sample
  genArrowArray(size, nullsRatio, generator, pa.callMethod("string"))


proc genVariableListArray[T](size: int,  nullsRatio: float, levels: int): TestTuple =
  doAssert levels > 0
  
  proc genlist(level: int): PyObject =
    if level == 1:
      let (pylist, _) = genPrimitiveArray(T, size, nullsRatio)
      pylist
    else:
      py.list([genlist(level-1),genlist(level-1),genlist(level-1)])
  
  proc typeGenerator(level: int): PyObject =
    if level == 0: pa.callMethod($T)
    else: pa.callMethod("list_", typeGenerator(level-1))
  
  proc generator(i: int): PyObject = genlist(levels)
  
  genArrowArray(size, nullsRatio, generator, typeGenerator(levels))


# ------------------------------------------------------------------


proc slice(testTuple: TestTuple, start, stop, step: int): (TestTuple) =
  let
    (pylist, aarray) = testTuple
  (pylist: pylist[py.callMethod("slice", start, stop + 1, step)],
   aarray: aarray.slice(start, stop, step))


# ------------------------------------------------------------------


test "dtype format parsing":
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


# ------------------------------------------------------------------


# https://github.com/apache/arrow/blob/97879eb970bac52d93d2247200b9ca7acf6f3f93/python/pyarrow/tests/test_cffi.py#L109
# https://github.com/apache/arrow/blob/488f084280fa5e2acea76dcb02dd0c3ee655f55b/python/pyarrow/array.pxi#L1312
proc genPrimitiveTest(T:typedesc, size: int, nullsRatio: float) =
  let
    tt = genPrimitiveArray(T, size, nullsRatio)
    (pylist, aarray) = tt
    start = int(size.float*0.2)
    stop = int(size.float*0.8)
    step = 2
    (pysliced, asliced) = tt.slice(start, stop, step)
  for (sliceDesc, aarray, pylist) in [
    (&"{aarray.low},{aarray.high},1", aarray, pylist), 
    (&"{start},{stop},{step}", asliced, pysliced)]:
    test &"primitive layout [{$T}] len:[{size}] slice:[{sliceDesc}] nullsRatio:[{nullsRatio}]":
      check[T,T](aarray, pylist)

proc genPrimitiveTestAux(T: typedesc) =
  genPrimitiveTest(T, 0, 0)
  genPrimitiveTest(T, 10, 0)
  genPrimitiveTest(T, 10, 0.5)
  
genPrimitiveTestAux(int16)
when not defined(skipSlowTests):
  genPrimitiveTestAux(int32)
  genPrimitiveTestAux(int64)
  genPrimitiveTestAux(uint16)
  genPrimitiveTestAux(uint32)
  genPrimitiveTestAux(uint64)
  genPrimitiveTestAux(float32)
  genPrimitiveTestAux(float64)


proc genVariableBinaryTest(size: int, nullsRatio: float) =
  let
    tt = genBinaryArray(size, nullsRatio)
    (pylist, aarray) = tt
    start = int(size.float*0.2)
    stop = int(size.float*0.8)
    step = 2
    (pysliced, asliced) = tt.slice(start, stop, step)
  for (sliceDesc, aarray, pylist) in [
    (&"{aarray.low},{aarray.high},1", aarray, pylist), 
    (&"{start},{stop},{step}", asliced, pysliced)]:
    test &"variable binary layout len:[{size}] slice:[{sliceDesc}] nullsRatio:[{nullsRatio}]":
      check[openArray[byte],string](aarray, pylist)

genVariableBinaryTest(0, 0)
genVariableBinaryTest(10, 0)
genVariableBinaryTest(10, 0.5)



proc genVariableListTest[T](size: int, nullsRatio: float, levels: int) =
  let
    tt = genVariableListArray[T](size, nullsRatio, levels)
    (pylist, aarray) = tt
    start = int(size.float*0.2)
    stop = int(size.float*0.8)
    step = 2
    (pysliced, asliced) = tt.slice(start, stop, step)
  for (sliceDesc, aarray, pylist) in [
    (&"{aarray.low},{aarray.high},1", aarray, pylist), 
    (&"{start},{stop},{step}", asliced, pysliced)]:
    test &"variable list array layout [{$T}] len:[{size}] slice:[{sliceDesc}] nullsRatio:[{nullsRatio}] levels:[{levels}]":
      check[T,T](aarray, pylist)

proc genVariableListTestAux[T] =
  for levels in 1..3:
    genVariableListTest[T](0, 0, levels)
    genVariableListTest[T](10, 0, levels)
    genVariableListTest[T](10, 0.5, levels)

genVariableListTestAux[int16]()
genVariableListTestAux[int32]()
genVariableListTestAux[int64]()
genVariableListTestAux[uint16]()
genVariableListTestAux[uint32]()
genVariableListTestAux[uint64]()
genVariableListTestAux[float32]()
genVariableListTestAux[float64]()


# # producers TODO
# test "produce [stack]":
#   var s = CSchema()
#   exportInt32Type(s.addr)
#   s.rootRelease()



# test "produce [heap]":
#   var s: ptr CSchema = cast[ptr CSchema](alloc(CSchema.sizeof))
#   exportInt32Type(s)
#   s[].rootRelease()
#   dealloc(s)