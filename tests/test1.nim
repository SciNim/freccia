import std/[sugar, random, unittest, strformat]
import nimpy
import nimpy/py_lib
import freccia

randomize(987)

# Tests rely on pyarrow
pyInitLibPath("/home/jack/.pyenv/versions/3.10.1/lib/libpython3.10.so.1.0")
let
  py = pyBuiltinsModule()
  pa = pyImport("pyarrow")



func expectedBufferCount(l: LayoutKind): int =
  case l
    of alNull: 0
    of alFixedList, alStruct, alUnionSparse: 1
    of alPrimitive, alVariableList, alUnionDense, alDictionary: 2
    of alVariableBinary: 3


proc check(cschema: CSchema, dtype: Type) =
  check parseType($cschema.format) == dtype
  

proc check(carray: CArray, dtype: Type, size: int, nulls: bool) =
  check carray.length == (if nulls: size*2 else: size)
  check carray.nullCount == (if nulls: size else: 0)
  check carray.bufferList.len == dtype.layout.expectedBufferCount
  check carray.childrenList.len == 0


proc genPrimitiveArray(T: typedesc, size: int, nulls: bool): (PyObject, ArrowArray) =
  var
    pylist = py.list()
    cschema: CSchema
    carray: CArray
  for i in 0..<size:
    discard pylist.append i.T
    if nulls: discard pylist.append py.None
  var pyarray = pa.`array`(pylist, type=pa.callMethod($T))
  discard pyarray.callMethod("_export_to_c", cast[int](carray.addr), cast[int](cschema.addr))
  let dtype = toType(T)
  cschema.check(dtype)
  carray.check(dtype, size, nulls)
  (pylist, initArrowArray(cschema, carray))


proc genBinaryArray(size: int, nulls: bool): (PyObject, ArrowArray) =
  const strSet = [
    "monkey", "elephant", "dolphin", "tarantula", "Supercalifragilisticexpialidocious", 
    "S̶̢̰̟̙̯͎̰͓̉̾̌͌̔̓͋̎̆͛̅͐͊͜u̴̮̦͇̼͙̦̗͑̐͐̏ͅͅp̴̨̝̮̙̔̚͠ê̸͕̥̿̎͋͑̍̿͝ṙ̴̪̳̮̠͇̱̠̀̎́̓̊̀̀̾̀̓̌̿̚͜ͅc̸̤̗̟͊̏̉͜a̶̧̨̟̗̠̟͖͕͕̣̺͇̙̤͊̑͝ͅl̴̛̫̒̂̆̃͗̐͑̄̍̓́͂̀̚i̴̛͖̾̋͑̎̾̈̏͌̒̿̚̚f̶̡̢͎̳͚̙̣̼͖̱̪̺̬́̀̊͋̉͌̇͋͒̄̿̈́͑̔r̴̨̛̛̗̟̙̅͐̿̓͋ǎ̷̙̰̜̭̠̼͑̋̇̌̐̈́͊̄͛̾͠g̸̢̧̯̗͉̘͎͔̉͒́̚i̸̥͍͈̗͍͍͎̘͌̔͊́̎̈́͋̆̕̕l̶̨̛̯̻̯̺̫̊̄̋̀̿̾̄̿̋ĩ̵̪̰̜̺̖̭͕̄̇͊̑̏̄̀̀̑̈́̍̋̚͝ͅs̵̛͖͚̼̀̂̐͐̚t̸̰̋̃̌̀̿̑͌ì̶̘͇͕̬̫̣̼͆̂̂͛͒̚c̶̢̛̲͉͕̮̩̗̼̱̭̘̳̈̑̌̓͝ę̷̪̰͇̬̿̿̉̃̾͜͠x̷̨̟̱͍̩͈̝̆͊́̑͐̕͜͝͝p̵̧̪̭̥͕̘̟̠̽̆̇̑̌̏̏͆͊̈́̈́̒͂̕͝i̶̧̨̦͖̟̼̮̠̠̤̬̺̙̜̓a̵̯̟̫̣͔̯̭͎̪͚̗̗̣͔͚͐̀͋̆͗̔͂̀̈͘l̶̤͎̞̓̂̊͑ͅḭ̷̫̻̖͚͓̦̻̦̱̭̑ͅd̶͈̾̂̎ò̷̧̢͈͖̹̹̭͓̲̐c̷̢̡̬͖̦̳̹̥͇̲̺͊͌͊̓̀̂̓̆̅ͅị̴̢͙̦̜̟̹̹̮́̀̎͑͒̆̈̎̀̀̂̉̕͜͝͝ơ̸̭̘̫̖̹͕͕̯̑͂̆͊̀́̇̋̋̇̈́̎͘͠ů̶̢̨̜̻̞̟̤̓̑s̶̨͇̦̫͙̞̥̽̊̋͗͋͛͛̈̈́̚̚͘", "丂ひｱ乇尺ᄃﾑﾚﾉｷ尺ﾑムﾉﾚﾉ丂ｲﾉᄃ乇ﾒｱﾉﾑﾚﾉりのᄃﾉのひ丂", 
    "👺☯  Ⓢ𝕦卩ⓔŘc𝐀𝕃ƗⒻℝ卂𝔾Ⓘ𝐋ⒾＳᵗ丨c𝑒𝕩𝔭Ꭵ𝐀Ĺ𝒾𝒹Ỗ𝒸丨𝐨𝔲ᔕ  👽☮"]
  var
    pylist = py.list()
    cschema: CSchema
    carray: CArray
  for i in 0..<size:
    discard pylist.append $i & "_" & strSet.sample
    if nulls: discard pylist.append py.None
  var pyarray = pa.`array`(pylist)
  discard pyarray.callMethod("_export_to_c", cast[int](carray.addr), cast[int](cschema.addr))
  let dtype = (if size > 0: toType(string) else: Type(kind: tkNull))
  cschema.check(dtype)
  carray.check(dtype, size, nulls)
  (pylist, initArrowArray(cschema, carray))


proc genVariableListArray(size: int, nulls: bool): (PyObject, ArrowArray) =
  proc genlist(): PyObject =
    let pylist = py.list()
    discard pylist.append 1
    discard pylist.append 2
    discard pylist.append 3
    pylist
  var
    pylist = py.list()
    cschema: CSchema
    carray: CArray
  for i in 0..<size:
    discard pylist.append py.list([genlist(),py.None,genlist()])
    if nulls: discard pylist.append py.None
  var pyarray = pa.`array`(pylist)
  discard pyarray.callMethod("_export_to_c", cast[int](carray.addr), cast[int](cschema.addr))
  (pylist, initArrowArray(cschema, carray))


proc toString(bytes: openArray[byte]): string =
  if bytes.len > 0:
    result = newString(bytes.len)
    copyMem(result[0].addr, bytes[0].unsafeAddr, bytes.len)
  else:
    result = ""


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


# https://github.com/apache/arrow/blob/97879eb970bac52d93d2247200b9ca7acf6f3f93/python/pyarrow/tests/test_cffi.py#L109
# https://github.com/apache/arrow/blob/488f084280fa5e2acea76dcb02dd0c3ee655f55b/python/pyarrow/array.pxi#L1312
proc genPrimitiveTestAux(T:typedesc, size: int, nulls: bool) =
  let
    (pylist, aarray) = genPrimitiveArray(T, size, nulls)
    sliceStart = int(size.float*0.2)
    sliceStop = int(size.float*0.8)
    sliceStep = 2
    asliced = aarray.slice(sliceStart, sliceStop, sliceStep)
    pysliced = pylist[py.callMethod("slice", sliceStart, sliceStop + 1, sliceStep)]
  for (sliceDesc, aa, pa) in [
    (&"{aarray.low},{aarray.high},1", aarray, pylist), 
    (&"{sliceStart},{sliceStop},{sliceStep}", asliced, pysliced)]:
    test &"primitive layout [{$T}] len:{size} slice:[{sliceDesc}]" & (if nulls: " with nulls" else: ""):
      let
        pylen = py.callMethod("len", pa).to(int)
      check aa.len == pylen
      check aa.low == 0
      check aa.high == pylen-1
      for i in 0..pylen-1:
        let 
          pyObj = pa[i]
          aVal = aa.item(i, T)
        if pyObj == py.None:
          check aVal == 0 # not strictly required
          check not aa.isValid(i)
        else:
          let pv = pyObj.to(T)
          check aa.isValid(i)
          check pv == aVal # https://github.com/nim-lang/Nim/issues/19426

proc genPrimitiveTest(T: typedesc) =
  genPrimitiveTestAux(T, 0, false)
  genPrimitiveTestAux(T, 10, false)
  genPrimitiveTestAux(T, 10, true)
  
genPrimitiveTest(int16)
when not defined(skipSlowTests):
  genPrimitiveTest(int32)
  genPrimitiveTest(int64)
  genPrimitiveTest(uint16)
  genPrimitiveTest(uint32)
  genPrimitiveTest(uint64)
  genPrimitiveTest(float32)
  genPrimitiveTest(float64)


proc genVariableBinaryTest(size: int, nulls: bool) =
  let
    (pylist, aarray) = genBinaryArray(size, nulls)
    sliceStart = int(size.float*0.2)
    sliceStop = int(size.float*0.8)
    sliceStep = 2
    asliced = aarray.slice(sliceStart, sliceStop, sliceStep)
    pysliced = pylist[py.callMethod("slice", sliceStart, sliceStop + 1, sliceStep)]
  for (sliceDesc, aa, pa) in [
      (&"{aarray.low},{aarray.high},1", aarray, pylist), 
      (&"{sliceStart},{sliceStop},{sliceStep}", asliced, pysliced)]:
    test &"variable binary layout len:{size} slice:[{sliceDesc}]" & (if nulls: " with nulls" else: ""):
      let
        pylen = py.len(pa).to(int)
      check aa.len == pylen
      check aa.low == 0
      check aa.high == pylen-1
      for i in 0..pylen-1:
        let 
          pyObj = pa[i]
          blob = aa.item(i, openArray[byte])
        if pyObj == py.None:
          check blob.len == 0
          check not aa.isValid(i)
        else:
          let pv = pyObj.to(string)
          check aa.isValid(i)
          check pv == blob.toString

genVariableBinaryTest(0, false)
genVariableBinaryTest(10, false)
genVariableBinaryTest(10, true)


# let 
#   (pylist, aarray) = genBinaryArray(10, false)
#   pysliced = pylist[py.callMethod("slice", 2, 9, 2)]
#   asliced = aarray.slice(2,8,2)

# for i in 0..<aarray.len:
#   echo &"N {i} - {aarray.item(i, openArray[byte]).toString}"
#   echo &"NP {i} - {pylist[i].to(string)}"

# for i in 0..<asliced.len:
#   echo &"S {i} - {asliced.item(i, openArray[byte]).toString}"
#   echo &"NS {i} - {pysliced[i].to(string)}"


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