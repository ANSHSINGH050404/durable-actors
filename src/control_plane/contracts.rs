use std::collections::{BTreeMap, HashSet};

use anyhow::{Context, Result, ensure};
use serde::{Deserialize, Serialize};
use serde_json::Value;

use super::admin::validate_component;

#[derive(Clone, Debug)]
pub(crate) struct PublicActorContract {
    document: Value,
    hash: String,
}

impl PublicActorContract {
    pub(crate) fn sandboxes(&self) -> Result<BTreeMap<String, SandboxOptions>> {
        let document: ContractDocument = serde_json::from_value(self.document.clone())?;
        Ok(document
            .actors
            .into_iter()
            .filter_map(|actor| actor.sandbox.map(|options| (actor.actor_name, options)))
            .collect())
    }

    pub(crate) fn new(document: Value) -> Result<Self> {
        let parsed: ContractDocument =
            serde_json::from_value(document.clone()).context("invalid public actor contract")?;
        parsed.validate()?;
        let document = canonical_json(document);
        let bytes = serde_json::to_vec(&document)?;
        ensure!(
            bytes.len() <= 4 * 1024 * 1024,
            "public actor contract exceeds 4 MiB"
        );
        let digest = aws_lc_rs::digest::digest(&aws_lc_rs::digest::SHA256, &bytes);
        let hash = format!(
            "sha256:{}",
            digest
                .as_ref()
                .iter()
                .map(|byte| format!("{byte:02x}"))
                .collect::<String>()
        );
        Ok(Self { document, hash })
    }

    pub(crate) fn document(&self) -> &Value {
        &self.document
    }
    pub(crate) fn hash(&self) -> &str {
        &self.hash
    }
}

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase")]
pub(crate) struct PublishedContract {
    pub contract_hash: String,
    pub contract: Value,
}

impl PublishedContract {
    pub(crate) fn rpc_methods(&self, actor_name: &str) -> Result<Vec<String>> {
        let document: ContractDocument = serde_json::from_value(self.contract.clone())?;
        let actor = document
            .actors
            .into_iter()
            .find(|actor| actor.actor_name == actor_name)
            .context("actor is not in the published contract")?;
        Ok(actor
            .rpc
            .methods
            .into_iter()
            .map(|method| method.name)
            .collect())
    }

    pub(crate) fn new(contract: &PublicActorContract) -> Self {
        Self {
            contract_hash: contract.hash().into(),
            contract: contract.document().clone(),
        }
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct ContractDocument {
    version: u32,
    actors: Vec<ActorApi>,
    typescript: Option<TypeScriptContract>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TypeScriptContract {
    declarations: String,
    dependencies: BTreeMap<String, String>,
}

impl ContractDocument {
    fn validate(&self) -> Result<()> {
        if let Some(typescript) = &self.typescript {
            ensure!(
                !typescript.declarations.trim().is_empty(),
                "public TypeScript declarations are required"
            );
            for (name, version) in &typescript.dependencies {
                let segments: Vec<_> = name.strip_prefix('@').unwrap_or(name).split('/').collect();
                ensure!(
                    ((segments.len() == 1 && !name.starts_with('@'))
                        || (segments.len() == 2 && name.starts_with('@')))
                        && segments.iter().all(|part| !part.is_empty()
                            && part.bytes().all(|b| b.is_ascii_lowercase()
                                || b.is_ascii_digit()
                                || b"._-".contains(&b)))
                        && !version.is_empty(),
                    "invalid public type dependency"
                );
            }
        }
        ensure!(
            self.version == 1,
            "unsupported public actor contract version {}",
            self.version
        );
        let mut names = HashSet::new();
        for actor in &self.actors {
            if let Some(sandbox) = &actor.sandbox {
                sandbox.validate()?;
            }
            validate_actor_name(&actor.actor_name)?;
            ensure!(
                names.insert(&actor.actor_name),
                "duplicate actor name {}",
                actor.actor_name
            );
            ensure!(
                actor.socket.version == 1 && actor.socket.actor_name == actor.actor_name,
                "actor and socket contract must match"
            );
            validate_schema(&actor.socket.schema)?;
            for kind in ["Metadata", "Incoming", "Outgoing", "State"] {
                ensure!(
                    actor.socket.schema["definitions"].get(kind).is_some(),
                    "missing socket type {kind}"
                );
            }
            let mut fields = HashSet::new();
            for field in &actor.socket.emittable {
                ensure!(fields.insert(field), "duplicate emittable field {field}");
                ensure!(
                    actor.socket.schema["definitions"]["State"]["properties"]
                        .get(field)
                        .is_some(),
                    "emittable field {field} is not public state"
                );
            }
            actor.rpc.validate()?;
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct ActorApi {
    actor_name: String,
    #[serde(rename = "description")]
    _description: Option<String>,
    sandbox: Option<SandboxOptions>,
    socket: SocketContract,
    rpc: RpcContract,
}

#[derive(Clone, Debug, Default, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
pub(crate) struct SandboxOptions {
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub cpu: Option<serde_json::Number>,
    #[serde(default, rename = "memoryMiB", skip_serializing_if = "Option::is_none")]
    pub memory_mib: Option<u32>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub regions: Option<Vec<String>>,
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub idle_timeout_ms: Option<u64>,
}

impl SandboxOptions {
    pub(crate) fn resources(
        &self,
        mut defaults: crate::sandbox::ResourceLimits,
    ) -> crate::sandbox::ResourceLimits {
        if let Some(cpu) = self.cpu.as_ref().and_then(serde_json::Number::as_f64) {
            defaults.cpu_millis = (cpu * 1000.0).round() as u32;
        }
        if let Some(memory) = self.memory_mib {
            defaults.memory_mib = memory;
        }
        defaults
    }

    pub(crate) fn validate(&self) -> Result<()> {
        if let Some(cpu) = &self.cpu {
            let cpu = cpu.as_f64().context("invalid sandbox CPU")?;
            ensure!(
                (0.1..=64.0).contains(&cpu) && (cpu * 1000.0 - (cpu * 1000.0).round()).abs() < 1e-8,
                "sandbox CPU must be 0.1–64 cores in increments of 0.001"
            );
        }
        if let Some(memory) = self.memory_mib {
            ensure!(
                (128..=262144).contains(&memory),
                "sandbox memory must be 128–262144 MiB"
            );
        }
        if let Some(timeout) = self.idle_timeout_ms {
            ensure!(
                (1..=86400000).contains(&timeout),
                "sandbox idle timeout must be 1–86400000 ms"
            );
        }
        if let Some(regions) = &self.regions {
            let unique: HashSet<_> = regions.iter().collect();
            ensure!(
                !regions.is_empty()
                    && unique.len() == regions.len()
                    && regions
                        .iter()
                        .all(|region| super::regions::ALL.contains(&region.as_str())),
                "sandbox regions must be a nonempty list of unique supported regions"
            );
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(rename_all = "camelCase", deny_unknown_fields)]
struct SocketContract {
    version: u32,
    actor_name: String,
    schema: Value,
    emittable: Vec<String>,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcContract {
    schema: Value,
    methods: Vec<RpcMethod>,
}

impl RpcContract {
    fn validate(&self) -> Result<()> {
        validate_schema(&self.schema)?;
        let mut names = HashSet::new();
        for method in &self.methods {
            validate_component("actor method", &method.name, 255)?;
            ensure!(
                names.insert(&method.name),
                "duplicate actor method {}",
                method.name
            );
            ensure!(
                ![
                    "constructor",
                    "then",
                    "connect",
                    "broadcast",
                    "onConnect",
                    "onMessage",
                    "onDisconnect"
                ]
                .contains(&method.name.as_str()),
                "reserved actor method {}",
                method.name
            );
            let mut optional_seen = false;
            for (index, parameter) in method.parameters.iter().enumerate() {
                ensure!(
                    !parameter.name.is_empty(),
                    "RPC parameter name must not be empty"
                );
                let kind = parameter.kind.validate(&self.schema)?;
                ensure!(
                    !parameter.rest
                        || (!parameter.optional
                            && index + 1 == method.parameters.len()
                            && kind["type"] == "array"
                            && !kind["items"].is_array()),
                    "rest parameter must be a final, required array parameter"
                );
                ensure!(
                    !optional_seen || parameter.optional || parameter.rest,
                    "required parameter follows optional parameter"
                );
                optional_seen |= parameter.optional;
            }
            if let RpcResult::Value { kind } = &method.result {
                kind.validate(&self.schema)?;
            }
        }
        Ok(())
    }
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcMethod {
    name: String,
    #[serde(rename = "description")]
    _description: Option<String>,
    parameters: Vec<RpcParameter>,
    result: RpcResult,
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct RpcParameter {
    name: String,
    optional: bool,
    rest: bool,
    #[serde(rename = "type")]
    kind: TypeReference,
}

#[derive(Deserialize)]
#[serde(tag = "kind", rename_all = "lowercase", deny_unknown_fields)]
enum RpcResult {
    Void,
    Value {
        #[serde(rename = "type")]
        kind: TypeReference,
    },
}

#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct TypeReference {
    #[serde(rename = "$ref")]
    reference: String,
}

impl TypeReference {
    fn validate<'a>(&self, schema: &'a Value) -> Result<&'a Value> {
        validate_reference(&self.reference, schema)
    }
}

fn validate_actor_name(name: &str) -> Result<()> {
    validate_component("actor name", name, 255)?;
    ensure!(
        name.bytes()
            .enumerate()
            .all(|(index, byte)| byte.is_ascii_alphabetic()
                || byte == b'_'
                || (index > 0 && byte.is_ascii_digit()))
            && !is_typescript_keyword(name),
        "actor name {name} cannot be emitted as a TypeScript identifier"
    );
    Ok(())
}

fn is_typescript_keyword(name: &str) -> bool {
    // Match the SDK's TypeScript scanner, including contextual keywords.
    [
        "abstract",
        "accessor",
        "any",
        "as",
        "asserts",
        "assert",
        "bigint",
        "boolean",
        "break",
        "case",
        "catch",
        "class",
        "continue",
        "const",
        "constructor",
        "debugger",
        "declare",
        "default",
        "defer",
        "delete",
        "do",
        "else",
        "enum",
        "export",
        "extends",
        "false",
        "finally",
        "for",
        "from",
        "function",
        "get",
        "if",
        "implements",
        "import",
        "in",
        "infer",
        "instanceof",
        "interface",
        "intrinsic",
        "is",
        "keyof",
        "let",
        "module",
        "namespace",
        "never",
        "new",
        "null",
        "number",
        "object",
        "package",
        "private",
        "protected",
        "public",
        "override",
        "out",
        "readonly",
        "require",
        "global",
        "return",
        "satisfies",
        "set",
        "static",
        "string",
        "super",
        "switch",
        "symbol",
        "this",
        "throw",
        "true",
        "try",
        "type",
        "typeof",
        "undefined",
        "unique",
        "unknown",
        "using",
        "var",
        "void",
        "while",
        "with",
        "yield",
        "async",
        "await",
        "of",
    ]
    .contains(&name)
}

fn validate_schema(schema: &Value) -> Result<()> {
    ensure!(
        schema.is_object() && schema["definitions"].is_object(),
        "contract schema must contain definitions"
    );
    validate_schema_node(schema, schema)
}

fn validate_schema_node(node: &Value, root: &Value) -> Result<()> {
    if node.is_boolean() {
        return Ok(());
    }
    let object = node
        .as_object()
        .context("contract schema must be an object or boolean")?;
    ensure!(
        !object.contains_key("$id") && !object.contains_key("id") && !object.contains_key("tsType"),
        "contract schemas cannot override type resolution"
    );
    if let Some(reference) = object.get("$ref") {
        validate_reference(
            reference.as_str().context("schema $ref must be a string")?,
            root,
        )?;
    }
    for key in ["definitions", "properties", "patternProperties"] {
        if let Some(children) = object.get(key) {
            for child in children
                .as_object()
                .context("schema properties must be an object")?
                .values()
            {
                validate_schema_node(child, root)?;
            }
        }
    }
    for key in [
        "items",
        "additionalItems",
        "additionalProperties",
        "contains",
        "propertyNames",
        "not",
        "if",
        "then",
        "else",
        "allOf",
        "anyOf",
        "oneOf",
    ] {
        if let Some(child) = object.get(key) {
            if let Some(children) = child.as_array() {
                for child in children {
                    validate_schema_node(child, root)?;
                }
            } else {
                validate_schema_node(child, root)?;
            }
        }
    }
    if let Some(dependencies) = object.get("dependencies") {
        for child in dependencies
            .as_object()
            .context("schema dependencies must be an object")?
            .values()
        {
            if !child.is_array() {
                validate_schema_node(child, root)?;
            }
        }
    }
    Ok(())
}

fn validate_reference<'a>(reference: &str, root: &'a Value) -> Result<&'a Value> {
    ensure!(
        reference.starts_with("#/definitions/"),
        "contract type references must be local definitions"
    );
    root.pointer(&reference[1..])
        .filter(|target| target.is_object() || target.is_boolean())
        .with_context(|| format!("contract type reference is missing: {reference}"))
}

fn canonical_json(value: Value) -> Value {
    match value {
        Value::Object(object) => {
            let mut entries: Vec<_> = object.into_iter().collect();
            entries.sort_by(|left, right| left.0.cmp(&right.0));
            Value::Object(
                entries
                    .into_iter()
                    .map(|(key, value)| (key, canonical_json(value)))
                    .collect(),
            )
        }
        Value::Array(values) => Value::Array(values.into_iter().map(canonical_json).collect()),
        other => other,
    }
}

#[cfg(test)]
#[path = "../../tests/unit/control_plane/contract_tests.rs"]
mod tests;
