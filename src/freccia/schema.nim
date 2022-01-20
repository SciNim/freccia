# https://github.com/apache/arrow/blob/master/format/Schema.fbs

const namespace* = "org.apache.arrow.flatbuf"

type
  # 8 bit: byte (int8), ubyte (uint8), bool
  # 16 bit: short (int16), ushort (uint16)
  # 32 bit: int (int32), uint (uint32), float (float32)
  # 64 bit: long (int64), ulong (uint64), double (float64)
  MetadataVersion* {.size: sizeof(int16).} = enum
    mvV1
    mvV2
    mvV3
    mvV4
    mvV5

  Feature* {.size: sizeof(int64).} = enum
    fUnused = 0,
    fDictionaryReplacement,
    fCompressedBody

  Null* = object

  Struct* = object

  List* = object

  LargeList* = object

  FixedSizeList* = object
    listSize*: int32

  Map* = object
    keySorted*: bool

  UnionMode* {.size: sizeof(int16).} = enum
    umSparse
    umDense
  
  Union* = object
    mode*: UnionMode
    typeIds*: seq[int32]

  Int* = object
    bitWidth*: int32
    isSigned*: bool

  Precision* {.size: sizeof(int16).} = enum
    pHalf
    pSingle
    pDouble

  FloatingPoint* = object
    precision*: Precision

  Utf8* = object

  Binary* = object

  LargeUtf8* = object

  LargeBinary* = object

  FixedSizeBinary* = object
    byteWidth*: int32

  Bool* = object

  Decimal* = object
    precision*: int32
    scale*: int32
    bitWidth*: int32 # default 128

  DateUnit* {.size: sizeof(int16).} = enum
    duDay
    duMillisecond

  Date* = object
    unit*: DateUnit # default duMillisecond

  TimeUnit* {.size: sizeof(int16).} = enum
    tuSecond
    tuMillisecond
    tuMicrosecond
    tuNanosecond

  Time* = object
    unit*: TimeUnit # default tuMillisecond
    bitWidth*: int32 # default 32

  Timestamp* = object
    unit*: TimeUnit
    zone*: string

  IntervalUnit* {.size: sizeof(int16).} = enum
    iuYearMonth
    iuDayTime
    iuMonthDayNano

  Interval* = object
    unit*: IntervalUnit

  Duration* = object
    unit*: TimeUnit # default tuMillisecond

  TypeKind* {.size: sizeof(int16).} = enum
    tkNull
    tkInt
    tkFloatingPoint
    tkBinary
    tkUtf8
    tkBool
    tkDecimal
    tkDate
    tkTime
    tkTimestamp
    tkInterval
    tkList
    tkStruct
    tkUnion
    tkFixedSizeBinary
    tkFixedSizeList
    tkMap
    tkDuration
    tkLargeBinary
    tkLargeUtf8
    tkLargeList

  Type* = object
    case kind*: TypeKind
    of tkNull: nullMeta*: Null
    of tkInt: intMeta*: Int
    of tkFloatingPoint: floatingPointMeta*: FloatingPoint
    of tkBinary: binaryMeta*: Binary
    of tkUtf8: utf8Meta*: Utf8
    of tkBool: boolMeta*: Bool
    of tkDecimal: decimalMeta*: Decimal
    of tkDate: dateMeta*: Date
    of tkTime: timeMeta*: Time
    of tkTimestamp: timestampMeta*: Timestamp
    of tkInterval: intervalMeta*: Interval
    of tkList: listMeta*: List
    of tkStruct: structMeta*: Struct
    of tkUnion: unionMeta*: Union
    of tkFixedSizeBinary: fixedSizeBinaryMeta*: FixedSizeBinary
    of tkFixedSizeList: fixedSizeListMeta*: FixedSizeList
    of tkMap: mapMeta*: Map
    of tkDuration: durationMeta*: Duration
    of tkLargeBinary: largeBinaryMeta*: LargeBinary
    of tkLargeUtf8: largeUtf8Meta*: LargeUtf8
    of tkLargeList: largeListMeta*: LargeList

  KeyValue* = object
    key*: string
    value*: string

  DictionaryKind* {.size: sizeof(int16).} = enum
    dkDenseArray

  DictionaryEncoding* = object
    id*: int64
    indexType*: Int
    isOrdered*: bool
    dictionaryKind*: DictionaryKind

  Field* = object
    name*: string
    nullable*: bool
    `type`*: Type
    dictionary*: DictionaryEncoding
    children*: seq[Field]
    customMetadata*: seq[KeyValue]

  Endianess* {.size: sizeof(int16).} = enum
    eLittle
    eBig

  Buffer* = object
    offset*: int64
    length*: int64

  Schema* = object
    endianess*: Endianess # default eLittle
    fields*: seq[Field]
    customMetadata*: seq[KeyValue]
    features*: seq[Feature]

  RootType* = Schema
