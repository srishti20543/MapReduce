from google.protobuf.internal import containers as _containers
from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Iterable as _Iterable, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RegisterRequest(_message.Message):
    __slots__ = ["in_dir", "mapper_ports", "mappers", "out_dir", "reducers", "typeOfRequest"]
    class TypeOfRequest(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    IN_DIR_FIELD_NUMBER: _ClassVar[int]
    InvertedIndex: RegisterRequest.TypeOfRequest
    MAPPERS_FIELD_NUMBER: _ClassVar[int]
    MAPPER_PORTS_FIELD_NUMBER: _ClassVar[int]
    NaturalJoin: RegisterRequest.TypeOfRequest
    OUT_DIR_FIELD_NUMBER: _ClassVar[int]
    REDUCERS_FIELD_NUMBER: _ClassVar[int]
    TYPEOFREQUEST_FIELD_NUMBER: _ClassVar[int]
    WordCount: RegisterRequest.TypeOfRequest
    in_dir: str
    mapper_ports: _containers.RepeatedScalarFieldContainer[int]
    mappers: int
    out_dir: str
    reducers: int
    typeOfRequest: RegisterRequest.TypeOfRequest
    def __init__(self, typeOfRequest: _Optional[_Union[RegisterRequest.TypeOfRequest, str]] = ..., in_dir: _Optional[str] = ..., out_dir: _Optional[str] = ..., mappers: _Optional[int] = ..., reducers: _Optional[int] = ..., mapper_ports: _Optional[_Iterable[int]] = ...) -> None: ...

class RegisterResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...
