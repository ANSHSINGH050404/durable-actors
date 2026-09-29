from __future__ import annotations

import inspect
import json
from collections.abc import Callable, Set
from dataclasses import MISSING, dataclass, is_dataclass
from types import GenericAlias
from typing import Annotated, Any, ClassVar, TypeVar, cast, get_args, get_origin, get_type_hints

from pydantic import BaseModel, TypeAdapter
from pydantic.json_schema import JsonSchemaMode
from typing_extensions import TypeAliasType

from .actor import Actor
from .guards import is_document, is_field, is_list
from .json import JsonValue
from .sandbox import sandbox_contract

Document = dict[str, Any]
HOOKS = {"on_connect", "on_message", "on_disconnect"}
RESERVED = {
    "get_connections",
    "broadcast",
    "get",
    "connect",
    "prepare_websocket",
    "subscribe",
}


@dataclass(frozen=True)
class Field:
    name: str
    adapter: TypeAdapter[Any]
    persisted: bool
    emittable: bool
    default: Any
    default_factory: Callable[[], object] | None


@dataclass(frozen=True)
class Method:
    signature: inspect.Signature
    parameters: dict[str, TypeAdapter[Any]]
    result: TypeAdapter[Any]
    returns_none: bool


@dataclass(frozen=True)
class Definition:
    actor: type[Actor[Any, Any, Any, Any]]
    fields: dict[str, Field]
    methods: dict[str, Method]
    reentrant_methods: set[str]
    socket_types: tuple[Any, Any, Any, Any]


def public_contract(actors: list[type[Actor[Any, Any, Any, Any]]]) -> Document:
    names = [actor.__name__ for actor in actors]
    if not names or len(names) != len(set(names)):
        raise ValueError("export at least one uniquely named actor")
    return {
        "version": 1,
        "actors": [
            actor_contract(describe_actor(a)) for a in sorted(actors, key=lambda a: a.__name__)
        ],
    }


_definitions: dict[type[Actor[Any, Any, Any, Any]], Definition] = {}


def describe_actor(actor: type[Actor[Any, Any, Any, Any]]) -> Definition:
    if actor not in _definitions:
        _definitions[actor] = _describe_actor(actor)
    return _definitions[actor]


def _describe_actor(actor: type[Actor[Any, Any, Any, Any]]) -> Definition:
    if Actor not in actor.__bases__:
        raise ValueError("actors must extend Actor directly")
    if actor.__init__ is not Actor.__init__:
        raise ValueError("use annotated field defaults instead of an actor constructor")
    hints = get_type_hints(actor, include_extras=True)
    fields = {
        name: read_field(actor, name, hint)
        for name, hint in hints.items()
        if get_origin(hint) is not ClassVar
    }
    methods: dict[str, Method] = {}
    reentrant_methods: set[str] = set()
    for name, value in vars(actor).items():
        if name.startswith("_") or name in fields or name in hints:
            continue
        if isinstance(value, (staticmethod, classmethod, property)):
            raise ValueError(
                f"{actor.__name__}.{name}: public accessors and static methods are unsupported"
            )
        if not inspect.isfunction(value):
            raise ValueError(
                f"{actor.__name__}.{name}: public members must be methods; fields require annotations"
            )
        if (
            inspect.iscoroutinefunction(value)
            or inspect.isgeneratorfunction(value)
            or inspect.isasyncgenfunction(value)
        ):
            raise ValueError(
                f"{actor.__name__}.{name}: actor methods and hooks must be synchronous def functions"
            )
        if name in RESERVED:
            raise ValueError(f"reserved actor method: {name}")
        if getattr(value, "__actor_reentrant__", False):
            reentrant_methods.add(name)
        if name not in HOOKS:
            methods[name] = read_method(value)
    socket_types = next(
        (
            get_args(base)
            for base in getattr(actor, "__orig_bases__", ())
            if get_origin(base) is Actor
        ),
        (JsonValue, JsonValue, JsonValue, str),
    )
    return Definition(actor, fields, dict(sorted(methods.items())), reentrant_methods, socket_types)


def actor_contract(definition: Definition) -> Document:
    roots: dict[str, TypeAdapter[Any]] = {}
    methods: list[Document] = []
    for name, method in definition.methods.items():
        parameters: list[Document] = []
        for index, (parameter_name, adapter) in enumerate(method.parameters.items()):
            parameter = method.signature.parameters[parameter_name]
            key = f"Method_{name}_Parameter_{index}"
            roots[key] = adapter
            parameters.append(
                {
                    "name": parameter_name,
                    "optional": parameter.default is not inspect.Parameter.empty,
                    "rest": parameter.kind is inspect.Parameter.VAR_POSITIONAL,
                    "type": {"$ref": f"#/definitions/{key}"},
                }
            )
        result: Document = {"kind": "void"}
        if not method.returns_none:
            key = f"Method_{name}_Result"
            roots[key] = method.result
            result = {"kind": "value", "type": {"$ref": f"#/definitions/{key}"}}
        methods.append(
            {
                "name": name,
                **documentation(getattr(definition.actor, name)),
                "parameters": parameters,
                "result": result,
            }
        )
    rpc_schema = schemas(roots, serialization={name for name in roots if name.endswith("_Result")})
    for name, method in definition.methods.items():
        for index, parameter in enumerate(method.signature.parameters.values()):
            node = rpc_schema["definitions"][f"Method_{name}_Parameter_{index}"]
            node["x-python-kind"] = parameter.kind.name
            if parameter.default is not inspect.Parameter.empty:
                node["default"] = encode(method.parameters[parameter.name], parameter.default)
    socket_roots = {
        name: adapter_for(hint)
        for name, hint in zip(("Metadata", "Incoming", "Outgoing", "Tag"), definition.socket_types)
    }
    public = {
        name: field
        for name, field in definition.fields.items()
        if field.persisted and not name.startswith("_")
    }
    socket_roots.update({f"Field_{name}": field.adapter for name, field in public.items()})
    socket_schema = schemas(
        socket_roots,
        serialization={"Outgoing", *(f"Field_{name}" for name in public)},
        reserved={"State"},
    )
    socket_schema["definitions"]["State"] = {
        "type": "object",
        "properties": {name: {"$ref": f"#/definitions/Field_{name}"} for name in public},
        "required": list(public),
    }
    return {
        "actorName": definition.actor.__name__,
        **documentation(definition.actor),
        **sandbox_contract(definition.actor),
        "rpc": {"schema": rpc_schema, "methods": methods},
        "socket": {
            "version": 1,
            "actorName": definition.actor.__name__,
            "schema": socket_schema,
            "emittable": [name for name, field in public.items() if field.emittable],
        },
    }


def documentation(value: object) -> Document:
    description = getattr(value, "__doc__", None)
    return {"description": inspect.cleandoc(description)} if description else {}


def read_field(actor: type[Actor[Any, Any, Any, Any]], name: str, hint: Any) -> Field:
    if name in RESERVED | HOOKS | {"id"}:
        raise ValueError(f"reserved actor field: {name}")
    default = getattr(actor, name, MISSING)
    factory: Callable[[], object] | None = None
    persisted, emittable = True, False
    if is_field(default):
        options = default
        mode = options.metadata.get("durable_actors")
        persisted, emittable = mode != "ephemeral", mode == "emitted"
        factory = options.default_factory if options.default_factory is not MISSING else None
        default = options.default
    if default is MISSING and factory is None:
        raise ValueError(f"{name}: actor fields require defaults")
    if emittable and name.startswith("_"):
        raise ValueError(f"{name}: emitted fields must be public")
    adapter: TypeAdapter[Any] = adapter_for(hint) if persisted else TypeAdapter[Any](Any)
    if persisted and factory is None:
        encode(adapter, default)
    return Field(name, adapter, persisted, emittable, default, factory)


def read_method(method: Any) -> Method:
    hints = get_type_hints(method, include_extras=True)
    signature = inspect.signature(method)
    all_parameters = list(signature.parameters.values())
    if not all_parameters or all_parameters[0].kind not in (
        inspect.Parameter.POSITIONAL_ONLY,
        inspect.Parameter.POSITIONAL_OR_KEYWORD,
    ):
        raise ValueError("RPC instance methods require a self parameter")
    parameters = all_parameters[1:]
    adapters: dict[str, TypeAdapter[Any]] = {}
    optional_seen = False
    for parameter in parameters:
        if parameter.name not in hints:
            raise ValueError(f"{method.__name__}.{parameter.name}: explicit annotation required")
        if parameter.kind is inspect.Parameter.VAR_KEYWORD:
            raise ValueError("RPC methods must use named parameters instead of **kwargs")
        if (
            optional_seen
            and parameter.default is inspect.Parameter.empty
            and parameter.kind is not inspect.Parameter.VAR_POSITIONAL
        ):
            raise ValueError("required RPC parameters must precede optional parameters")
        optional_seen |= parameter.default is not inspect.Parameter.empty
        hint = hints[parameter.name]
        if parameter.kind is inspect.Parameter.VAR_POSITIONAL:
            if parameter is not parameters[-1]:
                raise ValueError("variadic RPC parameters must be last")
            hint = GenericAlias(list, hint)
        adapters[parameter.name] = adapter_for(hint)
    if "return" not in hints:
        raise ValueError(f"{method.__name__}: explicit return annotation required")
    return Method(
        signature.replace(parameters=parameters),
        adapters,
        adapter_for(hints["return"]),
        hints["return"] is type(None),
    )


def adapter_for(hint: Any) -> TypeAdapter[Any]:
    validate_type(hint, set())
    adapter: TypeAdapter[Any] = TypeAdapter(hint)
    adapter.json_schema()
    return adapter


def schemas(
    roots: dict[str, TypeAdapter[Any]],
    *,
    serialization: Set[str] = frozenset(),
    reserved: Set[str] = frozenset(),
) -> Document:
    if not roots:
        return {"$schema": "http://json-schema.org/draft-07/schema#", "definitions": {}}
    modes: dict[str, JsonSchemaMode] = {
        name: "serialization" if name in serialization else "validation" for name in roots
    }
    references, schema = TypeAdapter.json_schemas(
        [(name, modes[name], adapter) for name, adapter in roots.items()]
    )
    definitions = schema.get("$defs", {})
    mapping: dict[str, str] = {}
    occupied = set(definitions) | set(roots) | reserved
    for name in definitions:
        candidate = name
        if name in roots or name in reserved:
            candidate += "Model"
            while candidate in occupied:
                candidate += "Model"
        mapping[name] = candidate
        occupied.add(candidate)
    renamed = {
        mapping[name]: rename_schema_refs(value, mapping) for name, value in definitions.items()
    }
    renamed.update(
        {name: rename_schema_refs(references[(name, modes[name])], mapping) for name in roots}
    )
    return cast(
        Document,
        draft7({"$schema": "http://json-schema.org/draft-07/schema#", "definitions": renamed}),
    )


def rename_schema_refs(value: Any, mapping: dict[str, str]) -> Any:
    if is_list(value):
        return [rename_schema_refs(item, mapping) for item in value]
    if not is_document(value):
        return value
    return {
        key: "#/$defs/" + mapping[item.removeprefix("#/$defs/")]
        if key == "$ref" and isinstance(item, str) and item.startswith("#/$defs/")
        else rename_schema_refs(item, mapping)
        for key, item in value.items()
    }


def draft7(value: Any) -> Any:
    if is_list(value):
        return [draft7(item) for item in value]
    if not is_document(value):
        return value
    result = {
        ("definitions" if key == "$defs" else key): (
            item.replace("#/$defs/", "#/definitions/")
            if key == "$ref" and isinstance(item, str)
            else draft7(item)
        )
        for key, item in value.items()
    }
    if "prefixItems" in result:
        tail = result.pop("items", False)
        result["items"] = result.pop("prefixItems")
        result["additionalItems"] = tail
    return result


def encode(adapter: TypeAdapter[Any], value: Any) -> Any:
    checked = adapter.validate_python(value, strict=True)
    encoded = adapter.dump_python(checked, mode="json", warnings="error", by_alias=True)
    return json.loads(json.dumps(encoded, allow_nan=False))


def decode(adapter: TypeAdapter[Any], value: Any) -> Any:
    return adapter.validate_json(json.dumps(value, allow_nan=False), strict=True)


def validate_type(hint: Any, seen: set[int]) -> None:
    if (
        hint is Any
        or hint is object
        or isinstance(hint, TypeVar)
        or hint in (list, dict, tuple, set)
    ):
        raise ValueError(
            "public types require concrete annotations; use JsonValue for arbitrary JSON"
        )
    if hint is JsonValue or id(hint) in seen:
        return
    seen.add(id(hint))
    if isinstance(hint, TypeAliasType):
        validate_type(hint.__value__, seen)
        return
    if get_origin(hint) is dict and get_args(hint)[0] is not str:
        raise ValueError("public dictionary keys must be strings")
    if get_origin(hint) is Annotated:
        validate_type(get_args(hint)[0], seen)
        return
    if isinstance(hint, type) and issubclass(hint, BaseModel):
        for field in hint.model_fields.values():
            validate_type(field.annotation, seen)
    elif (
        is_dataclass(cast(object, hint))
        or isinstance(hint, type)
        and hasattr(hint, "__required_keys__")
    ):
        for annotation in get_type_hints(cast(object, hint), include_extras=True).values():
            validate_type(annotation, seen)
    else:
        from typing import Literal

        if get_origin(hint) is not Literal:
            for child in get_args(hint):
                if child is not Ellipsis:
                    validate_type(child, seen)
