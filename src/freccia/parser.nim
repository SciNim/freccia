import std/[sugar]
import schema



# copying from parseutils to handle SomeSignedInt
proc parseInt[T:SomeSignedInt](s: string, b: var T, start = 0): int =
  template err = raise newException(ValueError, "Parsed integer outside of valid range")
  var
    sign: T = -1
    i = start
  if i < s.len:
    if s[i] == '+': inc(i)
    elif s[i] == '-':
      inc(i)
      sign = 1
  if i < s.len and s[i] in {'0'..'9'}:
    b = 0
    while i < s.len and s[i] in {'0'..'9'}:
      let c = T(ord(s[i]) - ord('0'))
      if b >= (low(T) + c) div 10:
        b = b * 10 - c
      else:
        err()
      inc(i)
      while i < s.len and s[i] == '_': inc(i) # underscores are allowed and ignored
    if sign == -1 and b == low(T):
      err()
    else:
      b = b * sign
      result = i - start


func parseType*(format: string): Type =
  template err = raise newException(ValueError, "invalid format")
  case format[0]:
  of 'n': result = Type(kind: tkNull)
  of 'b': result = Type(kind: tkBool)
  of 'c': result = Type(kind: tkInt, intMeta: Int(bitWidth: 8, isSigned: true))
  of 'C': result = Type(kind: tkInt, intMeta: Int(bitWidth: 8, isSigned: false))
  of 's': result = Type(kind: tkInt, intMeta: Int(bitWidth: 16, isSigned: true))
  of 'S': result = Type(kind: tkInt, intMeta: Int(bitWidth: 16, isSigned: false))
  of 'i': result = Type(kind: tkInt, intMeta: Int(bitWidth: 32, isSigned: true))
  of 'I': result = Type(kind: tkInt, intMeta: Int(bitWidth: 32, isSigned: false))
  of 'l': result = Type(kind: tkInt, intMeta: Int(bitWidth: 64, isSigned: true))
  of 'L': result = Type(kind: tkInt, intMeta: Int(bitWidth: 64, isSigned: false))
  of 'e': result = Type(kind: tkFloatingPoint, floatingPointMeta: FloatingPoint(precision: pHalf))
  of 'f': result = Type(kind: tkFloatingPoint, floatingPointMeta: FloatingPoint(precision: pSingle))
  of 'g': result = Type(kind: tkFloatingPoint, floatingPointMeta: FloatingPoint(precision: pDouble))
  of 'z': result = Type(kind: tkBinary)
  of 'Z': result = Type(kind: tkLargeBinary)
  of 'u': result = Type(kind: tkUtf8)
  of 'U': result = Type(kind: tkLargeUtf8)
  of 'd':
    var decimal: Decimal
    doAssert format.parseInt(decimal.precision, 2) > 0
    doAssert format.parseInt(decimal.scale, 5) > 0
    if format.parseInt(decimal.bitWidth, 8) == 0:
      decimal.bitWidth = 128
    result = Type(kind: tkDecimal, decimalMeta: decimal)
  of 'w':
    var fixedSizeBinary: FixedSizeBinary
    doAssert format.parseInt(fixedSizeBinary.byteWidth, 2) > 0
    result = Type(kind: tkFixedSizeBinary, fixedSizeBinaryMeta: fixedSizeBinary)
  of 't':
    case format[1]:
    of 'd':
      let date = case format[2]:
        of 'D': Date(unit: duDay)
        of 'm': Date(unit: duMillisecond)
        else: err
      result = Type(kind: tkDate, dateMeta: date)
    of 't':
      let time = case format[2]:
        of 's': Time(unit: tuSecond, bitWidth: 32)
        of 'm': Time(unit: tuMillisecond, bitWidth: 32)
        of 'u': Time(unit: tuMicrosecond, bitWidth: 64)
        of 'n': Time(unit: tuNanosecond, bitWidth: 64)
        else: err
      result = Type(kind: tkTime, timeMeta: time)
    of 's':
      let zone = format[4..^1]
      let timestamp = case format[2]:
        of 's': Timestamp(unit: tuSecond, zone: zone)
        of 'm': Timestamp(unit: tuMillisecond, zone: zone)
        of 'u': Timestamp(unit: tuMicrosecond, zone: zone)
        of 'n': Timestamp(unit: tuNanosecond, zone: zone)
        else: err
      result = Type(kind: tkTimestamp, timestampMeta: timestamp)
    of 'D':
      let duration = case format[2]:
      of 's': Duration(unit: tuSecond)
      of 'm': Duration(unit: tuMillisecond)
      of 'u': Duration(unit: tuMicrosecond)
      of 'n': Duration(unit: tuNanosecond)
      else: err 
      result = Type(kind: tkDuration, durationMeta: duration)
    of 'i':
      let interval = case format[2]:
      of 'M': Interval(unit: iuYearMonth)
      of 'D': Interval(unit: iuDayTime)
      of 'n': Interval(unit: iuMonthDayNano)
      else: err
      result = Type(kind: tkInterval, intervalMeta: interval)
    else: err
  of '+':
    case format[1]:
    of 'l': result = Type(kind: tkList)
    of 'L': result = Type(kind: tkLargeList)
    of 'w':
      var fixedSizeList: FixedSizeList
      doAssert format.parseInt(fixedSizeList.listSize, 3) > 0
      result = Type(kind: tkFixedSizeList, fixedSizeListMeta: fixedSizeList)
    of 's': result = Type(kind: tkStruct)
    of 'm': result = Type(kind: tkMap, mapMeta: Map(keySorted: false))
    of 'u':        
      var 
        start = 4
        typeIds = collect(newSeq):
          while start < format.len:
            var id: int32
            start += format.parseInt(id, start) + 1
            id           
      case format[2]:
      of 'd': result = Type(kind: tkUnion, unionMeta: Union(mode: umDense, typeIds: typeIds))
      of 's': result = Type(kind: tkUnion, unionMeta: Union(mode: umSparse, typeIds: typeIds))
      else: err
    else: err
  else: err