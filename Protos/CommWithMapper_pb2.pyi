from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class MappingRequest(_message.Message):
    __slots__ = ["directories", "ids", "index", "reducers", "typeOfRequest"]
    class TypeOfRequest(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    DIRECTORIES_FIELD_NUMBER: _ClassVar[int]
    IDS_FIELD_NUMBER: _ClassVar[int]
    INDEX_FIELD_NUMBER: _ClassVar[int]
    InvertedIndex: MappingRequest.TypeOfRequest
    NaturalJoin: MappingRequest.TypeOfRequest
    REDUCERS_FIELD_NUMBER: _ClassVar[int]
    TYPEOFREQUEST_FIELD_NUMBER: _ClassVar[int]
    WordCount: MappingRequest.TypeOfRequest
    directories: _containers.RepeatedScalarFieldContainer[str]
    ids: _containers.RepeatedScalarFieldContainer[str]
    index: int
    reducers: int
    typeOfRequest: MappingRequest.TypeOfRequest
    def __init__(self, directories: _Optional[_Iterable[str]] = ..., typeOfRequest: _Optional[_Union[MappingRequest.TypeOfRequest, str]] = ..., index: _Optional[int] = ..., reducers: _Optional[int] = ..., ids: _Optional[_Iterable[str]] = ...) -> None: ...

class Response(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...
