from __future__ import annotations

import ast
import json
import keyword
from copy import deepcopy
from pathlib import Path
from typing import Any, cast

from jsonschema import Draft7Validator

from .contract import Document
from .guards import is_document, is_list


def generate_client(contract: Document, output: Path) -> None:
    if contract.get("version") != 1 or not isinstance(contract.get("actors"), list):
        raise ValueError("unsupported actor contract")
    files: dict[str, str] = {}
    namespaces: list[str] = []
    names: list[str] = []
    for index, actor in enumerate(contract["actors"]):
        name = identifier(actor["actorName"])
        module = name.lower()
        if any(path in files for path in (f"_{module}.py", f"_{module}_models.py")):
            raise ValueError("actor names collide in Python modules")
        model_source, types, models = generate_models(actor)
        source = actor_client(actor, types)
        ast.parse(source)
        files[f"_{module}_models.py"] = model_source
        files[f"_{module}.py"] = source
        namespaces.append(actor_namespace(actor, types, models, index))
        names.append(name)
    files["actors.py"] = (
        '"""Typed actor namespaces; obtain an instance handle with actors.Name.get(actor_id)."""\n'
        "from __future__ import annotations\n"
        "import builtins as _builtins\n"
        "from typing import TypeAlias as _TypeAlias, Literal as _Literal\n"
        "from durable_actors.client import ActorTransport as _ActorTransport\n"
        "from durable_actors.generated import Unset as _Unset\n"
        "from durable_actors.proxy import SocketAuthorization as _SocketAuthorization, prepare_authorization as _prepare_authorization\n"
        "from durable_actors.client import SocketGrant as _SocketGrant\n"
        "from pydantic import TypeAdapter as _TypeAdapter\n"
        + "\n".join(namespaces)
        + f"\n__all__ = {names!r}\n"
    )
    files["__init__.py"] = proxy_module(names)
    files["py.typed"] = ""
    output.mkdir(parents=True, exist_ok=True)
    for name, source in files.items():
        (output / name).write_text(source)


def generate_models(actor: Document) -> tuple[str, dict[str, str], list[str]]:
    from datamodel_code_generator import DatetimeClassType, InputFileType, LiteralType, generate
    from datamodel_code_generator.enums import DataModelType

    definitions, properties = collect_schemas(actor)
    expose_state(actor, definitions, properties)
    expose_variadic_items(actor, definitions, properties)
    root_name = "ContractTypes"
    while root_name in definitions:
        root_name += "Root"
    schema = {
        "type": "object",
        "description": "Named wire types used by this generated actor client.",
        "properties": properties,
        "required": list(properties),
        "definitions": definitions,
    }
    mark_omittable(schema, definitions)
    source = generate(
        json.dumps(schema),
        input_file_type=InputFileType.JsonSchema,
        output_model_type=DataModelType.PydanticV2BaseModel,
        class_name=root_name,
        base_class="durable_actors.generated.ClientModel",
        field_extra_keys={"x-durable-actors-omittable", "x-durable-actors-nullable"},
        disable_timestamp=True,
        use_type_alias=True,
        use_union_operator=True,
        enum_field_as_literal=LiteralType.All,
        field_constraints=True,
        use_tuple_for_fixed_items=True,
        strict_nullable=True,
        output_datetime_class=DatetimeClassType.Datetime,
        formatters=[],
        use_annotated=True,
        use_standard_collections=True,
        use_schema_description=True,
        use_field_description=True,
    )
    if not isinstance(source, str):
        raise ValueError("expected a single generated models module")
    source = compatible_aliases(omittable_fields(source))
    models = [
        node.name
        for node in ast.parse(source).body
        if isinstance(node, ast.ClassDef) and node.name != root_name
    ]
    return source, root_types(source, root_name), models


def collect_schemas(actor: Document) -> tuple[Document, Document]:
    definitions: Document = {}
    properties: Document = {}
    for prefix, schema in (("Rpc", actor["rpc"]["schema"]), ("Socket", actor["socket"]["schema"])):
        Draft7Validator.check_schema(schema)
        roots = {
            name
            for name in schema["definitions"]
            if name.startswith("Method_") or name in {"Metadata", "Incoming", "Outgoing", "State"}
        }
        mapping = {
            name: prefix + name
            if name in roots or name in definitions and definitions[name] != definition
            else name
            for name, definition in schema["definitions"].items()
        }
        for name, definition in schema["definitions"].items():
            key = mapping[name]
            definitions[key] = rename_references(definition, mapping)
            if name in roots:
                properties[key] = {"$ref": "#/definitions/" + key}
    return definitions, properties


def expose_state(actor: Document, definitions: Document, properties: Document) -> None:
    definitions["SocketState"].setdefault("description", "Public persisted fields of the actor.")
    emitted = {
        name: value
        for name, value in definitions["SocketState"]["properties"].items()
        if name in actor["socket"]["emittable"]
    }
    required_state = [
        name for name in definitions["SocketState"].get("required", []) if name in emitted
    ]
    for name, required in (("SocketEmittedState", required_state), ("SocketStatePatch", [])):
        definitions[name] = {
            "type": "object",
            "properties": deepcopy(emitted),
            "required": required,
            "description": (
                "Complete current values of the actor's emitted fields."
                if name == "SocketEmittedState"
                else "Changed emitted fields; omitted fields are unchanged."
            ),
        }
        properties[name] = {"$ref": "#/definitions/" + name}


def expose_variadic_items(actor: Document, definitions: Document, properties: Document) -> None:
    for method in actor["rpc"]["methods"]:
        for parameter in method["parameters"]:
            if parameter["rest"]:
                name = "Rpc" + root_key(parameter["type"])
                node = definitions[name]
                seen: set[str] = set()
                while "$ref" in node:
                    reference = root_key(node)
                    if reference in seen:
                        raise ValueError("variadic parameter has a circular schema reference")
                    seen.add(reference)
                    node = definitions[reference]
                definitions[name + "Item"] = node["items"]
                properties[name + "Item"] = {"$ref": "#/definitions/" + name + "Item"}


def root_types(source: str, name: str) -> dict[str, str]:
    tree = ast.parse(source)
    root = next(node for node in tree.body if isinstance(node, ast.ClassDef) and node.name == name)
    defined = defined_names(tree)
    types: dict[str, str] = {}
    for node in root.body:
        if not isinstance(node, ast.AnnAssign) or not isinstance(node.target, ast.Name):
            continue
        key = next(
            (
                item.value.value
                for item in ast.walk(node)
                if isinstance(item, ast.keyword)
                and item.arg == "alias"
                and isinstance(item.value, ast.Constant)
                and isinstance(item.value.value, str)
            ),
            node.target.id,
        )
        annotation = node.annotation
        if (
            isinstance(annotation, ast.Subscript)
            and isinstance(annotation.value, ast.Name)
            and annotation.value.id == "Annotated"
        ):
            annotation = cast(ast.Tuple, annotation.slice).elts[0]
        types[key] = qualify(annotation, defined)
    return types


def actor_namespace(actor: Document, types: dict[str, str], models: list[str], index: int) -> str:
    name = identifier(actor["actorName"])
    client, model = f"_client_{index}", f"_models_{index}"
    lines = [
        f"from . import _{name.lower()} as {client}",
        f"from . import _{name.lower()}_models as {model}",
        "",
        f"class {name}:",
        docstring(actor.get("description") or f"Clients and types for {name} actors.", 4),
        *namespace_aliases(types, models, client, model),
        "",
        *get_method(client),
        "",
        *authorization_methods(name, types, model),
        "",
        *method_namespace(actor, types, model),
    ]
    return "\n".join(lines)


def namespace_aliases(
    types: dict[str, str], models: list[str], client: str, model: str
) -> list[str]:
    aliases = {
        "Stub": f"{client}.Stub",
        **{
            kind: namespace_type(types["Socket" + kind], model)
            for kind in ("Metadata", "Incoming", "Outgoing", "State", "EmittedState", "StatePatch")
        },
    }
    reserved = {*aliases, "Methods", "get", "Authorization", "prepare_websocket"}
    state_models = {
        types["Socket" + kind].removeprefix("_models.")
        for kind in ("State", "EmittedState", "StatePatch")
    }
    for name in models:
        if name in state_models:
            continue
        alias = name
        if alias in reserved:
            while alias in reserved or alias in models:
                alias += "Model"
        aliases[alias] = f"{model}.{name}"
        reserved.add(alias)
    return [f"    {key}: _TypeAlias = {value}" for key, value in aliases.items()]


def get_method(client: str) -> list[str]:
    return [
        "    @_builtins.staticmethod",
        f"    def get(actor_id: _builtins.str, transport: _ActorTransport | None = None) -> {client}.Stub:",
        docstring(
            "Return a synchronous actor handle without sending a request.\n\n"
            "Args:\n"
            "    actor_id: Identity of the actor to call.\n"
            "    transport: Optional caller-owned transport. Uses the SDK-managed\n"
            "        transport when omitted.\n\n"
            "The returned Stub provides the actor's typed RPC methods.",
            8,
        ),
        f"        return {client}.Stub(actor_id, transport)",
    ]


def authorization_methods(name: str, types: dict[str, str], model: str) -> list[str]:
    metadata = namespace_type(types["SocketMetadata"], model)
    return [
        f"    class Authorization(_SocketAuthorization[{metadata}]):",
        docstring(
            f"Backend-approved access to a {name} actor and its typed connection metadata.", 8
        ),
        f"        actor_name: _Literal[{name!r}] = {name!r}",
        "",
        "    @_builtins.staticmethod",
        f"    def prepare_websocket(authorization: {name}.Authorization, transport: _ActorTransport | None = None) -> _SocketGrant:",
        docstring(
            "Issue browser access after authenticating the user and authorizing this actor.", 8
        ),
        f"        return _prepare_authorization(_TypeAdapter({name}.Authorization).validate_python(authorization), transport)",
    ]


def proxy_module(names: list[str]) -> str:
    authorization = " | ".join(f"actors.{name}.Authorization" for name in names) or "_Never"
    return f'''"""Generated actor clients, authorization types, and browser access helpers."""
from . import actors as actors
from typing import TypeAlias as _TypeAlias, Never as _Never
from pydantic import TypeAdapter as _TypeAdapter
from durable_actors.client import ActorTransport as _ActorTransport, SocketGrant as _SocketGrant
from durable_actors.proxy import prepare_authorization as _prepare_authorization
from durable_actors import ActorSession as ActorSession, ActorSessionTransport as ActorSessionTransport, ActorSessionRejectedError as ActorSessionRejectedError

ActorAuthorization: _TypeAlias = {authorization}

class ActorProxy:
    """Issue WebSocket grants for the actors in this generated package."""

    @staticmethod
    def handle(authorization: ActorAuthorization, transport: _ActorTransport | None = None) -> _SocketGrant:
        """Issue a grant after the backend has authenticated and authorized the user."""
        return _prepare_authorization(_TypeAdapter(ActorAuthorization).validate_python(authorization), transport)
'''


def method_namespace(actor: Document, types: dict[str, str], model: str) -> list[str]:
    lines = [
        "    class Methods:",
        docstring("Argument tuples and return types for each actor RPC.", 8),
    ]
    for method in actor["rpc"]["methods"]:
        result = method["result"]
        returned = (
            "None"
            if result["kind"] == "void"
            else namespace_type(types["Rpc" + root_key(result["type"])], model)
        )
        lines.extend(
            [
                f"        class {identifier(method['name'])}:",
                docstring(
                    method.get("description")
                    or f"Types for {actor['actorName']}.{method['name']}.",
                    12,
                ),
                f"            Args: _TypeAlias = {argument_tuple(method, types, model)}",
                f"            Result: _TypeAlias = {returned}",
                "",
            ]
        )
    return lines


def namespace_type(hint: str, model: str) -> str:
    class Qualifier(ast.NodeTransformer):
        def visit_Name(self, node: ast.Name) -> ast.expr:
            if node.id == "_models":
                return ast.Name(id=model, ctx=ast.Load())
            if node.id in {
                "list",
                "dict",
                "tuple",
                "str",
                "int",
                "float",
                "bool",
                "set",
                "frozenset",
                "bytes",
                "object",
                "type",
            }:
                return ast.Attribute(
                    value=ast.Name(id="_builtins", ctx=ast.Load()), attr=node.id, ctx=ast.Load()
                )
            return node

    return ast.unparse(Qualifier().visit(ast.parse(hint, mode="eval").body))


def argument_tuple(method: Document, types: dict[str, str], model: str) -> str:
    alternatives: list[str] = []
    prefix: list[str] = []
    for parameter in method["parameters"]:
        key = "Rpc" + root_key(parameter["type"])
        hint = namespace_type(types[key + ("Item" if parameter["rest"] else "")], model)
        if parameter["optional"]:
            alternatives.append("_builtins.tuple[" + (", ".join(prefix) or "()") + "]")
            hint += " | _Unset"
        prefix.append(f"*_builtins.tuple[{hint}, ...]" if parameter["rest"] else hint)
    alternatives.append("_builtins.tuple[" + (", ".join(prefix) or "()") + "]")
    return " | ".join(alternatives)


def actor_client(actor: Document, types: dict[str, str]) -> str:
    name = identifier(actor["actorName"])
    lines = [
        "from __future__ import annotations",
        "import json as _json",
        "from collections.abc import Callable as _Callable",
        "from pydantic import TypeAdapter as _TypeAdapter",
        "from durable_actors.client import ActorTransport as _ActorTransport, SocketGrant as _SocketGrant, default_client as _default_client",
        "from durable_actors.connection import Connection as _Connection",
        "from durable_actors.subscription import Subscription as _Subscription",
        "from durable_actors.generated import UNSET as _UNSET, Unset as _Unset, arguments as _arguments, argument as _argument",
        f"from . import _{name.lower()}_models as _models",
        "",
        "class Stub:",
        docstring(actor.get("description") or f"Synchronous client for {name} actors.", 4),
        "    def __init__(self, actor_id: str, transport: _ActorTransport | None = None) -> None:",
        docstring(
            "Address an actor using the SDK-managed transport by default.\n\n"
            "Args:\n"
            "    actor_id: Identity of the actor to call.\n"
            "    transport: Optional custom transport; the caller owns its lifetime.",
            8,
        ),
        "        self._actor_id = actor_id",
        "        self._transport = transport if transport is not None else _default_client()",
        "",
    ]
    for method in actor["rpc"]["methods"]:
        lines.extend(rpc_method(name, method, actor["rpc"]["schema"], types))
    lines.extend(socket_methods(name, types))
    lines.extend(subscription_method(actor, types))
    return "\n".join(lines)


def rpc_method(actor: str, method: Document, schema: Document, types: dict[str, str]) -> list[str]:
    name = identifier(method["name"])
    if name in {"connect", "prepare_websocket", "subscribe", "broadcast"}:
        raise ValueError(f"reserved actor method: {name}")
    params, values, defaults = method_parameters(method["parameters"], schema, types)
    result = method["result"]
    returned = "None" if result["kind"] == "void" else types["Rpc" + root_key(result["type"])]
    lines = [
        f"    def {name}(_self{', ' if params else ''}{', '.join(params)}) -> {returned}:",
        docstring(
            method.get("description")
            or f"Invoke {actor}.{name} synchronously.\n\n"
            "Arguments and results are validated against the actor contract.\n"
            "Remote failures raise ActorInvocationError.",
            8,
        ),
        f"        _args = _arguments([{', '.join(values)}], [{', '.join(defaults)}])",
        f"        _result = _self._transport.invoke({actor!r}, _self._actor_id, {name!r}, _args)",
    ]
    if returned != "None":
        lines.extend(
            [
                f"        _adapter: _TypeAdapter[{returned}] = _TypeAdapter({returned})",
                "        return _adapter.validate_json(_json.dumps(_result), strict=True)",
            ]
        )
    return [*lines, ""]


def docstring(value: str, indent: int) -> str:
    escaped = "\n".join(json.dumps(line, ensure_ascii=False)[1:-1] for line in value.split("\n"))
    if "\n" in escaped:
        escaped += "\n"
    return "\n".join(" " * indent + line for line in ('"""' + escaped + '"""').split("\n"))


def method_parameters(
    parameters: list[Document], schema: Document, types: dict[str, str]
) -> tuple[list[str], list[str], list[str]]:
    params: list[str] = []
    values: list[str] = []
    defaults: list[str] = []
    keyword_started = False
    for index, parameter in enumerate(parameters):
        name = identifier(parameter["name"])
        key = root_key(parameter["type"])
        node = schema["definitions"][key]
        kind = node.get("x-python-kind", "POSITIONAL_OR_KEYWORD")
        hint = types["Rpc" + key + ("Item" if parameter["rest"] else "")]
        if parameter["rest"]:
            params.append(f"*{name}: {hint}")
            values.append(f"*[_argument(item, _TypeAdapter({hint})) for item in {name}]")
            keyword_started = True
            continue
        if kind == "KEYWORD_ONLY" and not keyword_started:
            params.append("*")
            keyword_started = True
        params.append(f"{name}: {hint}" + (" | _Unset = _UNSET" if parameter["optional"] else ""))
        values.append(f"_argument({name}, _TypeAdapter({hint}))")
        defaults.append(
            f"_json.loads({json.dumps(json.dumps(node['default']))})"
            if "default" in node
            else "_UNSET"
        )
        next_kind = (
            schema["definitions"][root_key(parameters[index + 1]["type"])].get("x-python-kind")
            if index + 1 < len(parameters)
            else None
        )
        if kind == "POSITIONAL_ONLY" and next_kind != "POSITIONAL_ONLY":
            params.append("/")
    return params, values, defaults


def socket_methods(actor: str, types: dict[str, str]) -> list[str]:
    metadata, incoming, outgoing = (
        types["Socket" + kind] for kind in ("Metadata", "Incoming", "Outgoing")
    )
    state, patch = types["SocketEmittedState"], types["SocketStatePatch"]
    connection = f"_Connection[{incoming}, {outgoing}, {state}, {patch}]"
    return [
        f"    def broadcast(self, message: {outgoing}) -> None:",
        docstring(
            "Broadcast a typed application message to every connection without persisting it.", 8
        ),
        f"        self._transport.broadcast({actor!r}, self._actor_id, _argument(message, _TypeAdapter({outgoing})))",
        "",
        f"    def prepare_websocket(self, metadata: {metadata}) -> _SocketGrant:",
        docstring(
            "Authorize a WebSocket connection without opening it.\n\n"
            "Args:\n"
            "    metadata: Typed metadata passed to the actor's on_connect hook.\n\n"
            "Returns:\n"
            "    A grant containing the connection URL and admission deadline.\n"
            "    Accepted connections remain authorized until they close.",
            8,
        ),
        f"        return self._transport.prepare_websocket({actor!r}, self._actor_id, _argument(metadata, _TypeAdapter({metadata})))",
        "",
        f"    def connect(self, metadata: {metadata}) -> {connection}:",
        docstring(
            "Open a typed, synchronous WebSocket connection.\n\n"
            "Args:\n"
            "    metadata: Typed metadata passed to the actor's on_connect hook.\n\n"
            "Use send() for application messages and receive() or iteration for\n"
            "messages and state events. Close the connection explicitly or use with.\n"
            "For complete emitted-state callbacks, use subscribe() when available.",
            8,
        ),
        "        grant = self.prepare_websocket(metadata)",
        f"        return {connection}.open(grant, _TypeAdapter({incoming}), _TypeAdapter({outgoing}), _TypeAdapter({state}), _TypeAdapter({patch}))",
        "",
    ]


def subscription_method(actor: Document, types: dict[str, str]) -> list[str]:
    if not actor["socket"]["emittable"]:
        return []
    metadata, state = types["SocketMetadata"], types["SocketEmittedState"]
    schema = actor["socket"]["schema"]
    default = (
        " = None"
        if nullable({"$ref": "#/definitions/Metadata"}, schema["definitions"], set())
        else ""
    )
    return [
        f"    def subscribe(self, callback: _Callable[[{state}], None], *, metadata: {metadata}{default}, on_error: _Callable[[Exception], None] | None = None) -> _Subscription[{state}]:",
        docstring(
            "Subscribe to complete typed snapshots of emitted state.\n\n"
            "Args:\n"
            "    callback: Receives the initial state and merged updates, serially\n"
            "        on a background thread.\n"
            "    metadata: Connection metadata; may be omitted only when nullable.\n"
            "    on_error: Handles receiving, validation, or callback failures on\n"
            "        the receiver thread. Without a handler, failures are logged.\n\n"
            "Returns:\n"
            "    A subscription to close() when finished, also usable as a context manager.\n\n"
            "Setup failures raise directly. Later failures stop the subscription.\n"
            "The receiver does not keep the process alive or reconnect automatically.",
            8,
        ),
        f"        return _Subscription(self.connect(metadata), callback, _TypeAdapter({state}), on_error=on_error)",
        "",
    ]


def root_key(reference: Document) -> str:
    value = reference.get("$ref")
    if not isinstance(value, str) or not value.startswith("#/definitions/"):
        raise ValueError("method types must reference contract definitions")
    return value.removeprefix("#/definitions/")


def identifier(name: str) -> str:
    if not name.isidentifier() or keyword.iskeyword(name) or name.startswith("_"):
        raise ValueError(f"cannot generate Python identifier {name!r}")
    return name


def rename_references(value: Any, mapping: dict[str, str]) -> Any:
    if is_document(value):
        result: Document = {}
        for key, item in value.items():
            if key == "$ref":
                if not isinstance(item, str) or not item.startswith("#/definitions/"):
                    raise ValueError("contract references must be local")
                name = item.removeprefix("#/definitions/")
                if name not in mapping:
                    raise ValueError("contract reference is missing")
                result[key] = "#/definitions/" + mapping[name]
            else:
                result[key] = rename_references(item, mapping)
        return result
    if is_list(value):
        return [rename_references(item, mapping) for item in value]
    return value


def defined_names(tree: ast.Module) -> set[str]:
    names: set[str] = set()
    nodes = [
        child
        for node in tree.body
        for child in ([*node.body, *node.orelse] if isinstance(node, ast.If) else [node])
    ]
    for node in nodes:
        if isinstance(node, (ast.ClassDef, ast.FunctionDef)):
            names.add(node.name)
        elif isinstance(node, ast.ImportFrom):
            names.update(alias.asname or alias.name for alias in node.names)
        elif isinstance(node, ast.AnnAssign) and isinstance(node.target, ast.Name):
            names.add(node.target.id)
        elif isinstance(node, ast.Assign):
            names.update(target.id for target in node.targets if isinstance(target, ast.Name))
    return names


def qualify(node: ast.expr, names: set[str]) -> str:
    class Qualifier(ast.NodeTransformer):
        def visit_Name(self, node: ast.Name) -> ast.expr:
            if node.id in names:
                return ast.Attribute(
                    value=ast.Name(id="_models", ctx=ast.Load()), attr=node.id, ctx=ast.Load()
                )
            return node

    return ast.unparse(Qualifier().visit(node))


def compatible_aliases(source: str) -> str:
    # Keep recursive and None aliases named at runtime so unions remain evaluable.
    tree = ast.parse(source)
    aliases = False
    for index, node in enumerate(tree.body):
        if not (
            isinstance(node, ast.Assign)
            and isinstance(node.value, ast.Call)
            and isinstance(node.value.func, ast.Name)
            and node.value.func.id == "TypeAliasType"
            and len(node.targets) == 1
            and isinstance(node.targets[0], ast.Name)
        ):
            continue
        aliases = True
        checked = ast.AnnAssign(
            target=node.targets[0],
            annotation=ast.Name(id="TypeAlias", ctx=ast.Load()),
            value=node.value.args[1],
            simple=1,
        )
        recursive = any(
            isinstance(child, ast.Constant)
            and isinstance(child.value, str)
            and node.targets[0].id in child.value
            for child in ast.walk(node.value.args[1])
        )
        tree.body[index] = (
            ast.If(test=ast.Name(id="TYPE_CHECKING", ctx=ast.Load()), body=[checked], orelse=[node])
            if recursive
            or isinstance(node.value.args[1], ast.Constant)
            and node.value.args[1].value is None
            else checked
        )
    if aliases:
        index = (
            1
            if isinstance(tree.body[0], ast.ImportFrom) and tree.body[0].module == "__future__"
            else 0
        )
        tree.body.insert(
            index,
            ast.ImportFrom(
                module="typing",
                names=[ast.alias(name="TYPE_CHECKING"), ast.alias(name="TypeAlias")],
                level=0,
            ),
        )
    return ast.unparse(ast.fix_missing_locations(tree)) + "\n"


def mark_omittable(value: Any, definitions: Document) -> None:
    if is_list(value):
        for item in value:
            mark_omittable(item, definitions)
    elif is_document(value):
        required = value.get("required", [])
        for name, property in value.get("properties", {}).items():
            if name not in required and "default" not in property:
                property["x-durable-actors-omittable"] = True
                property["x-durable-actors-nullable"] = nullable(property, definitions, set())
        for item in list(value.values()):
            mark_omittable(item, definitions)


def nullable(schema: Document, definitions: Document, seen: set[str]) -> bool:
    kind = schema.get("type", [])
    if kind == "null" or isinstance(kind, list) and "null" in kind:
        return True
    if "$ref" in schema:
        key = root_key(schema)
        if key not in seen:
            return nullable(definitions[key], definitions, seen | {key})
    return any(
        nullable(item, definitions, seen)
        for item in schema.get("anyOf", []) + schema.get("oneOf", [])
    )


def omittable_fields(source: str) -> str:
    tree = ast.parse(source)
    changed = False
    for node in ast.walk(tree):
        if not isinstance(node, ast.AnnAssign) or not isinstance(node.annotation, ast.Subscript):
            continue
        annotation = node.annotation
        if (
            not isinstance(annotation.value, ast.Name)
            or annotation.value.id != "Annotated"
            or not isinstance(annotation.slice, ast.Tuple)
        ):
            continue
        for field in annotation.slice.elts[1:]:
            if isinstance(field, ast.Call) and rewrite_omittable(field, annotation.slice):
                node.value = ast.Name(id="_UNSET", ctx=ast.Load())
                changed = True
    if changed:
        tree.body.insert(
            1,
            ast.ImportFrom(
                module="durable_actors.generated",
                names=[
                    ast.alias(name="UNSET", asname="_UNSET"),
                    ast.alias(name="Unset", asname="_Unset"),
                    ast.alias(name="is_unset", asname="_is_unset"),
                ],
                level=0,
            ),
        )
    return ast.unparse(ast.fix_missing_locations(tree)) + "\n"


def rewrite_omittable(field: ast.Call, annotation: ast.Tuple) -> bool:
    metadata = next(
        (
            item
            for item in field.keywords
            if item.arg == "json_schema_extra" and isinstance(item.value, ast.Dict)
        ),
        None,
    )
    if metadata is None:
        return False
    extras = ast.literal_eval(metadata.value)
    if not extras.pop("x-durable-actors-omittable", False):
        return False
    allows_null = extras.pop("x-durable-actors-nullable", False)
    field.keywords.remove(metadata)
    if extras:
        field.keywords.append(
            ast.keyword(arg="json_schema_extra", value=ast.parse(repr(extras), mode="eval").body)
        )
    field.keywords.append(
        ast.keyword(arg="exclude_if", value=ast.Name(id="_is_unset", ctx=ast.Load()))
    )
    original = annotation.elts[0]
    annotation.elts[0] = ast.BinOp(
        left=original if allows_null else without_null(original),
        op=ast.BitOr(),
        right=ast.Name(id="_Unset", ctx=ast.Load()),
    )
    return True


def without_null(node: ast.expr) -> ast.expr:
    if isinstance(node, ast.BinOp) and isinstance(node.op, ast.BitOr):
        if isinstance(node.right, ast.Constant) and node.right.value is None:
            return without_null(node.left)
        if isinstance(node.left, ast.Constant) and node.left.value is None:
            return without_null(node.right)
        return ast.BinOp(
            left=without_null(node.left), op=ast.BitOr(), right=without_null(node.right)
        )
    if (
        isinstance(node, ast.Subscript)
        and isinstance(node.value, ast.Name)
        and node.value.id == "Optional"
    ):
        return node.slice
    return node


if __name__ == "__main__":
    import sys

    generate_client(json.load(sys.stdin), Path(sys.argv[1]))
