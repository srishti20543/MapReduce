from google.protobuf.internal import enum_type_wrapper as _enum_type_wrapper
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class RegisterRequest(_message.Message):
    __slots__ = ["typeOfRequest"]
    class TypeOfRequest(int, metaclass=_enum_type_wrapper.EnumTypeWrapper):
        __slots__ = []
    InvertedIndex: RegisterRequest.TypeOfRequest
    NaturalJoin: RegisterRequest.TypeOfRequest
    TYPEOFREQUEST_FIELD_NUMBER: _ClassVar[int]
    WordCount: RegisterRequest.TypeOfRequest
    typeOfRequest: RegisterRequest.TypeOfRequest
    def __init__(self, typeOfRequest: _Optional[_Union[RegisterRequest.TypeOfRequest, str]] = ...) -> None: ...

class RegisterResponse(_message.Message):
    __slots__ = ["status"]
    STATUS_FIELD_NUMBER: _ClassVar[int]
    status: str
    def __init__(self, status: _Optional[str] = ...) -> None: ...
