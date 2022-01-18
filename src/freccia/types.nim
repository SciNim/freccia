# From https://arrow.apache.org/docs/format/CDataInterface.html

#[
  #define ARROW_FLAG_DICTIONARY_ORDERED 1
  #define ARROW_FLAG_NULLABLE 2
  #define ARROW_FLAG_MAP_KEYS_SORTED 4

  struct ArrowSchema {
    // Array type description
    const char* format;
    const char* name;
    const char* metadata;
    int64_t flags;
    int64_t n_children;
    struct ArrowSchema** children;
    struct ArrowSchema* dictionary;

    // Release callback
    void (*release)(struct ArrowSchema*);
    // Opaque producer-specific data
    void* private_data;
  };

  struct ArrowArray {
    // Array data description
    int64_t length;
    int64_t null_count;
    int64_t offset;
    int64_t n_buffers;
    int64_t n_children;
    const void** buffers;
    struct ArrowArray** children;
    struct ArrowArray* dictionary;

    // Release callback
    void (*release)(struct ArrowArray*);
    // Opaque producer-specific data
    void* private_data;
  };
]#

#[

  # From futhark

  type                                                                                                                                                                                                               
    Arrowarray_1610612963* = object                                                                                                                                                                                  
      length*: cint            ## Generated based on /home/jack/nim/freccia/csrc/interface.h:21:8                                                                                                                    
      nullcount*: cint                                                                                                                                                                                               
      offset*: cint                                                                                                                                                                                                  
      nbuffers*: cint                                                                                                                                                                                                
      nchildren*: cint                                                                                                                                                                                               
      buffers*: ptr pointer                                                                                                                                                                                          
      children*: ptr ptr Arrowarray_1610612965                                                                                                                                                                       
      dictionary*: ptr Arrowarray_1610612965                                                                                                                                                                         
      release*: proc (a0: ptr Arrowarray_1610612965): void {.cdecl.}                                                                                                                                                 
      privatedata*: pointer                                                                                                                                                                                          
                                                                                                                                                                                                                    
    Arrowschema_1610612966* = object                                                                                                                                                                                 
      format*: cstring         ## Generated based on /home/jack/nim/freccia/csrc/interface.h:5:8                                                                                                                     
      name*: cstring                                                                                                                                                                                                 
      metadata*: cstring                                                                                                                                                                                             
      flags*: cint                                                                                                                                                                                                   
      nchildren*: cint                                                                                                                                                                                               
      children*: ptr ptr Arrowschema_1610612967                                                                                                                                                                      
      dictionary*: ptr Arrowschema_1610612967                                                                                                                                                                        
      release*: proc (a0: ptr Arrowschema_1610612967): void {.cdecl.}
]#

type
  ArrowFlag* {.size: sizeof(cint).} = enum
    afDictionaryOrdered = 1
    afNullable
    afMapKeysSorted
  ArrowSchema* = object
    format: cstring
    name: cstring
    metadata: ptr UncheckedArray[char]
    flags: set[ArrowFlag]
    nChildren: cint
    children: ptr UncheckedArray[ptr ArrowSchema]
    dictionary: ptr ArrowSchema
    release: proc(a: ptr ArrowSchema): void {.cdecl.}
    privateData: pointer
  ArrowArray* = object
    length: cint
    nullCount: cint
    offset: cint
    nBuffers: cint
    nChildren: cint
    buffers: ptr UncheckedArray[pointer]
    children: ptr UncheckedArray[ptr ArrowArray]
    dictionary: ptr ArrowArray
    release: proc(a: ptr ArrowArray): void {.cdecl.}
    privateData: pointer
  ArrowBaseStructure = ArrowSchema | ArrowArray


# Getters
proc release*[T: ArrowBaseStructure](s: T): void = 
  s.release(s.unsafeAddr)

proc getLength*(aarray: ArrowArray): int =
  aarray.length

proc getNbuffers*(aarray: ArrowArray): int =
  aarray.nBuffers

proc getFormat*(aschema: ArrowSchema): string =
  $aschema.format

# test 
proc releaseExported[T: ptr ArrowBaseStructure](bs: T) {.cdecl.} =
  when T is ArrowSchema:
    doAssert not bs.format.isNil

  for i in 0..<bs.nchildren:
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