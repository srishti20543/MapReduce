from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReducerRequest(_message.Message):
    __slots__ = ["directory", "index", "mappers", "typeOfRequest"]
    class TypeOfRequest(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    DIRECTORY_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    InvertedIndex: ReducerRequest.TypeOfRequest
    MAPPERS_FIELD_NUMBER: _ClassVar[int]
    NaturalJoin: ReducerRequest.TypeOfRequest
    TYPEOFREQUEST_FIELD_NUMBER: _ClassVar[int]
    WordCount: ReducerRequest.TypeOfRequest
    directory: str
    index: int
    mappers: int
    typeOfRequest: ReducerRequest.TypeOfRequest
    def __init__(self, directory: _Optional[str] = ..., typeOfRequest: _Optional[_Union[ReducerRequest.TypeOfRequest, str]] = ..., index: _Optional[int] = ..., mappers: _Optional[int] = ...) -> None: ...

class ReducerResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...
