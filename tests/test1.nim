import std/[unittest, sugar]
import nimpy
import nimpy/py_lib
import freccia



test "do the stack":
  check:
    compiles:
      var s = ArrowSchema()
      exportInt32Type(s.unsafeAddr)
      dump repr s
      s.release

test "do the heap":
  check:
    compiles:
      var s: ptr ArrowSchema = cast[ptr ArrowSchema](alloc(ArrowSchema.sizeof))
      exportInt32Type(s)
      dump repr s
      s[].release
      dealloc(s)

# https://github.com/apache/arrow/blob/97879eb970bac52d93d2247200b9ca7acf6f3f93/python/pyarrow/tests/test_cffi.py#L109
# https://github.com/apache/arrow/blob/488f084280fa5e2acea76dcb02dd0c3ee655f55b/python/pyarrow/array.pxi#L1312
test "pyarrow to nim":
  pyInitLibPath("/home/jack/.pyenv/versions/3.10.1/lib/libpython3.10.so.1.0")
  var
    pa = pyImport("pyarrow")
    years = pa.`array`([1990, 2000, 1995, 2000, 1995], type=pa.callMethod("int16"))
    sch: ArrowSchema
    arr: ArrowArray
  discard years.callMethod("_export_to_c", cast[int](arr.unsafeAddr), cast[int](sch.unsafeAddr))
  check sch.getFormat == "s"
  check arr.getLength == 5


