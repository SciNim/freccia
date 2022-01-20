import cinterface
import binding


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