from typing import Any, Mapping, Type, Union

from sqlalchemy import ARRAY, ColumnElement, exc, types
from sqlalchemy.sql import type_api

from .json import YqlJSON  # noqa: F401


class UInt64(types.Integer):
    __visit_name__ = "uint64"


class UInt32(types.Integer):
    __visit_name__ = "uint32"


class UInt16(types.Integer):
    __visit_name__ = "uint16"


class UInt8(types.Integer):
    __visit_name__ = "uint8"


class Int64(types.Integer):
    __visit_name__ = "int64"


class Int32(types.Integer):
    __visit_name__ = "int32"


class Int16(types.Integer):
    __visit_name__ = "int32"


class Int8(types.Integer):
    __visit_name__ = "int8"


class ListType(ARRAY):
    __visit_name__ = "list_type"


class StructType(types.TypeEngine[Mapping[str, Any]]):
    __visit_name__ = "struct_type"

    def __init__(self, fields_types: Mapping[str, Union[Type[types.TypeEngine], Type[types.TypeDecorator]]]):
        self.fields_types = fields_types

    @property
    def python_type(self):
        return dict

    def compare_values(self, x, y):
        return x == y


class Lambda(ColumnElement):
    __visit_name__ = "lambda"

    def __init__(self, func):
        if not callable(func):
            raise exc.ArgumentError("func must be callable")

        self.type = type_api.NULLTYPE
        self.func = func
