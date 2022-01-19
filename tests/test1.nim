import std/[unittest]
import nimpy
import nimpy/py_lib
import freccia


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
    let fmtType = formatTypeMap['g']
    check fmtType == atFloat64
    let aaa= 56.0

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
  var years = pa.`array`(pyears, type=pa.callMethod("int16"))
  discard years.callMethod("_export_to_c", cast[int](arr.unsafeAddr), cast[int](sch.unsafeAddr))

  check sch.format == "s"
  check sch.datatype == atInt16
  check arr.length == 7
  check arr.nullCount == 2
  check arr.buffers.len == 2
  check arr.children.len == 0
  echo $sch
  echo $arr


# producers
test "do the stack":
  var s = ArrowSchema()
  exportInt32Type(s.unsafeAddr)
  s.release

test "do the heap":
  var s: ptr ArrowSchema = cast[ptr ArrowSchema](alloc(ArrowSchema.sizeof))
  exportInt32Type(s)
  s[].release
  dealloc(s)