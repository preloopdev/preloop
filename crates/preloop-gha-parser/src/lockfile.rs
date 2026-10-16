//! GitHub Actions dependency lockfile, `.github/workflows/actions.lock`.
//!
//! A lockfile pins each action a workflow depends on, transitively, to a commit.
//! [`ActionsLock::parse`] reads schema versions v0.0.1 through v0.0.3 under the rules of
//! `github.com/github/actions-lockfile` (`go/pkg/lockfile`), and is stricter where its
//! documentation says so. [`pin_key_for_uses`] classifies `uses:` strings the way
//! `github.com/github/gh-actions-lock` does, through the same package. Parsing never touches the
//! network or writes files.

use std::collections::BTreeMap;
use std::fmt;

use indexmap::IndexSet;
use serde_yaml::{Mapping, Value};

/// Repository-relative path of the lockfile.
pub const LOCKFILE_PATH: &str = ".github/workflows/actions.lock";

/// Largest lockfile [`ActionsLock::parse`] accepts, in bytes. Matches `MaxParseSize` in the Go
/// package.
pub const MAX_LOCKFILE_BYTES: usize = 1 << 20;

/// Most `dependencies` entries [`ActionsLock::parse`] accepts. Each pin of a job's workflow is
/// re-verified against GitHub per job, so the count bounds that work.
pub const MAX_DEPENDENCIES: usize = 1000;

/// Newest schema version this parser reads.
const LATEST_VERSION: &str = "v0.0.3";

/// The numbers in [`LATEST_VERSION`].
const LATEST_NUMBERS: [u64; 3] = [0, 0, 3];

/// Host that v0.0.1 and v0.0.2 lockfiles describe. Both were dotcom-only, so their dependencies
/// are normalized to it, as the Go parser does.
const DOTCOM_HOST: &str = "github.com";

/// Why a lockfile was rejected.
#[derive(Debug, thiserror::Error)]
pub enum LockfileError {
    /// The input is larger than [`MAX_LOCKFILE_BYTES`]; nothing was parsed.
    #[error("lockfile is {bytes} bytes, over the {max}-byte limit")]
    TooLarge {
        /// Size of the rejected input in bytes.
        bytes: usize,
        /// The limit in force.
        max: usize,
    },
    /// The `dependencies` map has more than [`MAX_DEPENDENCIES`] entries; none were checked.
    #[error("lockfile lists {count} dependencies, over the {max}-dependency limit")]
    TooManyDependencies {
        /// Number of entries in the rejected `dependencies` map.
        count: usize,
        /// The limit in force.
        max: usize,
    },
    /// The document is empty, contains only comments, or has no `version` key.
    #[error("dependency lockfile version is required")]
    MissingVersion,
    /// `version` is a well-formed version newer than this parser reads.
    #[error(
        "lockfile version {version} is newer than this parser supports ({latest}); upgrade the tool that reads this lockfile"
    )]
    FutureVersion {
        /// The version the file declares.
        version: String,
        /// The newest version this parser reads.
        latest: &'static str,
    },
    /// `version` is not one of v0.0.1, v0.0.2 or v0.0.3.
    #[error("unsupported dependency lockfile version {version:?}")]
    UnsupportedVersion {
        /// The version the file declares.
        version: String,
    },
    /// The YAML is malformed, or a value has the wrong type. `line` and `column` are set when
    /// serde_yaml reports a location.
    #[error("malformed lockfile: {message}")]
    Malformed {
        /// serde_yaml's message, which includes its location.
        message: String,
        /// 1-indexed line of the problem, when serde_yaml reports one.
        line: Option<usize>,
        /// 1-indexed column of the problem, when serde_yaml reports one.
        column: Option<usize>,
    },
    /// A `workflows` entry or a `uses` list names a pin with no `dependencies` entry, so the
    /// lockfile cannot account for that dependency.
    #[error("{referrer} refers to {pin_key:?}, which has no dependencies entry")]
    DanglingPin {
        /// The referring entry, for example `workflow ".github/workflows/ci.yml"`.
        referrer: String,
        /// The canonical pin key that is missing.
        pin_key: String,
    },
    /// Any other rule violation: unknown or missing fields, invalid pins, commits, hostnames or
    /// IDs, conflicting duplicate entries, unsafe workflow paths, and `uses` cycles.
    #[error("{0}")]
    Invalid(String),
}

/// A parsed and validated lockfile.
///
/// Keys are canonical pin keys: owner and repo lowercased, ref verbatim, because git refs are
/// case-sensitive. Each `workflows` entry is one workflow's flat, transitive pin list, and every
/// pin in it has a `dependencies` entry, so [`ActionsLock::dependency`] finds every pin the file
/// lists.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ActionsLock {
    version: String,
    workflows: BTreeMap<String, Vec<String>>,
    dependencies: BTreeMap<String, LockedDependency>,
}

/// One `dependencies` entry: the commit a pin resolves to, and the identity of its repository.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockedDependency {
    /// Canonical pin key, `OWNER/REPO@REF`.
    pub pin_key: String,
    /// The lockfile `ref`. For v0.0.1 files this is the `tag`, else the `branch`, else the ref
    /// written in the pin key.
    pub reference: String,
    /// The commit the pin resolves to.
    pub commit: LockedCommit,
    /// Numeric ID of the owner. A changed ID means the name now belongs to another owner.
    pub owner_id: u64,
    /// Numeric ID of the repository. A changed ID means the name now belongs to another
    /// repository.
    pub repo_id: u64,
    /// GitHub host that owns the dependency. `None` means the home host, the tenant on a GHE.com
    /// data-residency instance or github.com elsewhere. v0.0.1 and v0.0.2 files always carry
    /// `Some("github.com")`.
    pub hostname: Option<String>,
    /// Canonical pin keys of the action's own direct dependencies (`uses:`). Empty for leaf
    /// actions.
    pub uses: Vec<String>,
}

/// A commit digest in lockfile form, `ALGORITHM-HEX`, for example `sha1-34e114876b0b...`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct LockedCommit {
    /// `sha1` or `sha256`.
    pub algorithm: String,
    /// Lowercase hex digest. This is the commit's git object name.
    pub hex: String,
}

impl LockedCommit {
    /// The hex digest, which is the git object name of the commit.
    pub fn as_git_sha(&self) -> &str {
        &self.hex
    }
}

impl fmt::Display for LockedCommit {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{}-{}", self.algorithm, self.hex)
    }
}

impl ActionsLock {
    /// Parses and validates the text of an `actions.lock` file.
    ///
    /// Accepts `version` values `v0.0.1`, `v0.0.2` and `v0.0.3`. Contents are normalized as the Go
    /// parser normalizes them: v0.0.1 keys lose their `:algo-hex` suffix and `tag` or `branch`
    /// become the reference; v0.0.1 and v0.0.2 dependencies get hostname `github.com`; `workflows`
    /// and `uses` entries are canonicalized. A key whose `ref` is a full commit SHA may carry any
    /// non-empty body `ref`, because the body ref is then the symbolic ref the SHA was found under.
    /// A workflow's pin list is a set: repeats are dropped at their first occurrence, which the
    /// Go parser does not do.
    ///
    /// This parser is stricter than the Go one in these ways:
    ///
    /// - Owner and repo must use GitHub's name characters (ASCII letters, digits, `-`, `_` and
    ///   `.`, and not `.` or `..`), because they become API path segments. Refs may not contain
    ///   control characters.
    /// - A workflow path key must name a file. Absolute paths, `..` segments, backslashes, colons,
    ///   control characters and keys that reduce to nothing are rejected. Leading `./`, `.` and
    ///   empty segments are dropped, so `./.github/workflows/ci.yml` and
    ///   `.github/workflows/ci.yml` name one workflow, and two keys that normalize to one path are
    ///   rejected.
    /// - Every `workflows` pin and every `uses` entry must have a `dependencies` entry, and the
    ///   `uses` graph must be acyclic.
    /// - Numbers in string fields must be quoted: `Value` drops their text (`1.10` becomes `1.1`).
    /// - A stream of more than one YAML document is rejected. The Go package's `yaml.Unmarshal`
    ///   reads only the first.
    ///
    /// serde_yaml's value tree keeps no source positions. [`LockfileError::Malformed`] therefore
    /// carries a line and column when serde_yaml reports one, and the other variants name the
    /// offending key or field instead.
    ///
    /// # Errors
    ///
    /// Returns [`LockfileError`] for oversized input, a missing or unsupported `version`, malformed
    /// YAML, and any rule above.
    pub fn parse(source: &str) -> Result<ActionsLock, LockfileError> {
        if source.len() > MAX_LOCKFILE_BYTES {
            return Err(LockfileError::TooLarge {
                bytes: source.len(),
                max: MAX_LOCKFILE_BYTES,
            });
        }
        if is_blank(source) {
            return Err(LockfileError::MissingVersion);
        }
        let document: Value = serde_yaml::from_str(source).map_err(malformed)?;
        let root = match document {
            Value::Mapping(root) => root,
            Value::Null => return Err(LockfileError::MissingVersion),
            _ => return Err(invalid("lockfile must be a YAML mapping")),
        };
        let version = match root.get("version") {
            None => return Err(LockfileError::MissingVersion),
            Some(Value::String(declared)) => declared.clone(),
            Some(_) => return Err(invalid("dependency lockfile version must be a string")),
        };
        let schema = Schema::from_version(&version)?;
        for name in root.keys() {
            match name.as_str() {
                Some("version" | "workflows" | "dependencies") => {}
                Some(unknown) => {
                    return Err(invalid(format!("unknown lockfile field {unknown:?}")));
                }
                None => return Err(invalid("lockfile field names must be strings")),
            }
        }
        let dependencies = match root.get("dependencies") {
            None | Some(Value::Null) => BTreeMap::new(),
            Some(Value::Mapping(entries)) => parse_dependencies(schema, entries)?,
            Some(_) => return Err(invalid("dependencies must be a mapping")),
        };
        let workflows = match root.get("workflows") {
            None | Some(Value::Null) => BTreeMap::new(),
            Some(Value::Mapping(entries)) => parse_workflows(schema, entries)?,
            Some(_) => return Err(invalid("workflows must be a mapping")),
        };
        check_references(&workflows, &dependencies)?;
        Ok(ActionsLock {
            version,
            workflows,
            dependencies,
        })
    }

    /// The `version` the file declares, such as `v0.0.2`.
    ///
    /// The Go package rewrites `File.Version` to the latest schema after parsing. This reports the
    /// declared value instead, so a caller can tell which format a file was written in. Contents
    /// are normalized to the current model whatever the version (see [`ActionsLock::parse`]).
    pub fn version(&self) -> &str {
        &self.version
    }

    /// Whether the lockfile onboards the workflow at `workflow_path`, a repository-relative path.
    ///
    /// A workflow is onboarded exactly when its path is a `workflows` entry, even one with no pins.
    /// The path is normalized as in [`ActionsLock::parse`], so a leading `./` is ignored. Paths
    /// that cannot name a repository file never match.
    pub fn is_onboarded(&self, workflow_path: &str) -> bool {
        self.workflow_pins(workflow_path).is_some()
    }

    /// The flat, transitive pin keys (`OWNER/REPO@REF`) of an onboarded workflow, in file order.
    /// Each key appears once, at its first occurrence.
    ///
    /// `None` when the workflow is not onboarded. An onboarded workflow with no dependencies
    /// returns an empty slice.
    pub fn workflow_pins(&self, workflow_path: &str) -> Option<&[String]> {
        let path = normalize_workflow_path(workflow_path).ok()?;
        self.workflows.get(&path).map(Vec::as_slice)
    }

    /// The onboarded workflow paths, in ascending order: the normalized `workflows` keys.
    ///
    /// A lockfile with no `workflows` entries onboards nothing and yields an empty iterator.
    pub fn workflow_paths(&self) -> impl Iterator<Item = &str> {
        self.workflows.keys().map(String::as_str)
    }

    /// The `dependencies` entry for `pin_key`.
    ///
    /// Owner and repo match case-insensitively, and the ref matches exactly.
    pub fn dependency(&self, pin_key: &str) -> Option<&LockedDependency> {
        let pin = parse_pin(pin_key)?;
        self.dependencies.get(&pin.canonical())
    }

    /// The `dependencies` strings for an onboarded workflow's job message: one per entry of
    /// [`ActionsLock::workflow_pins`], in the same order. `None` when the workflow is not
    /// onboarded.
    ///
    /// Each string is `[HOST/]OWNER/REPO@REF:ALGO-HEX`: the canonical pin key, a colon, then the
    /// commit in lockfile digest form, for example `actions/checkout@v4:sha1-<40 hex>`. `HOST/` is
    /// the entry's `hostname`. v0.0.1 and v0.0.2 files always set it to `github.com`; a v0.0.3
    /// entry without a `hostname` has no prefix and means the home host.
    ///
    /// The official runner forwards these strings without parsing them: `GetDownloadInfoAsync` in
    /// `src/Runner.Worker/ActionManager.cs` sends them as `actions_dependencies` (see
    /// `src/Sdk/WebApi/WebApi/LaunchContracts.cs`). Its only samples of the grammar are
    /// `actions/checkout@v4:sha256-abc123` (`src/Test/L0/Sdk/RSWebApi/AgentJobRequestMessageL0.cs`)
    /// and `github.com/actions/checkout@v4:sha256-abc123`
    /// (`src/Test/L0/Worker/ActionManagerL0.cs`).
    /// The digest here is the lockfile `commit` verbatim, `sha1` in practice; the samples' `sha256`
    /// label is a placeholder. The Launch service that parses these strings is not in this
    /// repository, so this form follows the samples, not a published grammar.
    pub fn wire_dependencies(&self, workflow_path: &str) -> Option<Vec<String>> {
        let pins = self.workflow_pins(workflow_path)?;
        // `parse` guarantees that every workflow pin has a dependency entry, so no pin is dropped.
        Some(
            pins.iter()
                .filter_map(|pin| self.dependencies.get(pin))
                .map(wire_entry)
                .collect(),
        )
    }
}

/// The job-message string for one locked entry; see [`ActionsLock::wire_dependencies`].
fn wire_entry(dependency: &LockedDependency) -> String {
    match &dependency.hostname {
        Some(host) => format!("{host}/{}:{}", dependency.pin_key, dependency.commit),
        None => format!("{}:{}", dependency.pin_key, dependency.commit),
    }
}

/// Classifies a `uses:` string and returns the lockfile pin key that covers it.
///
/// Mirrors `ParseActionRef` in `go/pkg/lockfile/uses.go`, which gh-actions-lock uses to decide what
/// a lockfile pins. The string is trimmed first. The result is `None` for:
///
/// - `./` local paths and `docker://` images, which the lockfile does not cover;
/// - `${` expressions, which GitHub rejects in `uses:`;
/// - `$/` self-repository references, which are inherently pinned and need no entry;
/// - reusable workflow files (`owner/repo/.github/workflows/x.yml@ref`), which gh-actions-lock
///   never pins. GitHub's lockfile documentation says they are pinned, so lifting this is a
///   contract change: it must land together with the policy's SHA-only rule and the resolver
///   using the locked commit. Their dependencies are covered by the callee repository's own
///   lockfile;
/// - anything else that is not `owner/repo[/path]@ref` with GitHub name characters and a ref
///   without `:` or control characters.
///
/// A subpath is dropped, because pins are repository-granular: `actions/cache/save@v4` maps to
/// `actions/cache@v4`. Owner and repo are lowercased, as the lockfile keys them. The ref is kept as
/// written, because git refs are case-sensitive, and it may itself contain `@` or `/`.
pub fn pin_key_for_uses(uses: &str) -> Option<String> {
    let uses = uses.trim();
    if uses.is_empty()
        || uses.chars().any(is_control_char)
        || uses.starts_with("./")
        || uses.starts_with("docker://")
        || uses.contains("${")
    {
        return None;
    }
    let (path, reference) = uses.split_once('@')?;
    let mut segments = path.splitn(3, '/');
    let owner = segments.next()?;
    let repo = segments.next()?;
    if let Some(subpath) = segments.next()
        && (!is_valid_subpath(subpath) || is_reusable_workflow_file(subpath))
    {
        return None;
    }
    pin_parts(owner, repo, reference).map(|pin| pin.canonical())
}

/// The schema versions this parser reads, chosen by the file's `version` string.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Schema {
    V001,
    V002,
    V003,
}

impl Schema {
    fn from_version(version: &str) -> Result<Schema, LockfileError> {
        match version {
            "v0.0.1" => Ok(Schema::V001),
            "v0.0.2" => Ok(Schema::V002),
            "v0.0.3" => Ok(Schema::V003),
            _ => match parse_schema_version(version) {
                Some(parsed) if parsed > LATEST_NUMBERS => Err(LockfileError::FutureVersion {
                    version: version.to_owned(),
                    latest: LATEST_VERSION,
                }),
                _ => Err(LockfileError::UnsupportedVersion {
                    version: version.to_owned(),
                }),
            },
        }
    }

    /// v0.0.1 and v0.0.2 describe github.com only, so their dependencies default to it.
    fn dotcom_only(self) -> bool {
        matches!(self, Schema::V001 | Schema::V002)
    }

    /// Whether `name` is an action field this schema defines.
    fn allows_field(self, name: &str) -> bool {
        match self {
            Schema::V001 => matches!(
                name,
                "tag" | "branch" | "commit" | "owner_id" | "repo_id" | "uses"
            ),
            Schema::V002 => matches!(name, "ref" | "commit" | "owner_id" | "repo_id" | "uses"),
            Schema::V003 => matches!(
                name,
                "hostname" | "ref" | "commit" | "owner_id" | "repo_id" | "uses"
            ),
        }
    }
}

/// Reads `vMAJOR.MINOR.PATCH` (the `v` optional) as numbers, as the Go package's `schemaVersionRE`
/// does.
fn parse_schema_version(version: &str) -> Option<[u64; 3]> {
    let digits = version.strip_prefix('v').unwrap_or(version);
    let mut parts = digits.split('.');
    let mut numbers = [0_u64; 3];
    for slot in &mut numbers {
        let part = parts.next()?;
        if part.is_empty() || !part.bytes().all(|byte| byte.is_ascii_digit()) {
            return None;
        }
        *slot = part.parse().ok()?;
    }
    parts.next().is_none().then_some(numbers)
}

/// Whether the source has nothing but blank lines and comments. serde_yaml reports such input as
/// an empty stream rather than a null document, so it is classified here.
fn is_blank(source: &str) -> bool {
    source
        .lines()
        .map(str::trim)
        .all(|line| line.is_empty() || line.starts_with('#'))
}

fn invalid(message: impl Into<String>) -> LockfileError {
    LockfileError::Invalid(message.into())
}

fn malformed(error: serde_yaml::Error) -> LockfileError {
    let location = error.location();
    LockfileError::Malformed {
        message: error.to_string(),
        line: location.as_ref().map(|at| at.line()),
        column: location.as_ref().map(|at| at.column()),
    }
}

fn empty_field(field: &str, raw_key: &str) -> LockfileError {
    invalid(format!(
        "action field {field:?} must not be empty for dependency {raw_key:?}"
    ))
}

/// The fields of one action body, keyed by field name.
type Fields<'a> = BTreeMap<&'a str, &'a Value>;

fn parse_dependencies(
    schema: Schema,
    entries: &Mapping,
) -> Result<BTreeMap<String, LockedDependency>, LockfileError> {
    // Checked before any entry is read, so an oversized map costs no per-entry work.
    if entries.len() > MAX_DEPENDENCIES {
        return Err(LockfileError::TooManyDependencies {
            count: entries.len(),
            max: MAX_DEPENDENCIES,
        });
    }
    let mut dependencies: BTreeMap<String, LockedDependency> = BTreeMap::new();
    for (key, body) in entries {
        let raw = key
            .as_str()
            .ok_or_else(|| invalid("dependency keys must be strings"))?;
        let pin = parse_pin_key(schema, raw).ok_or_else(|| {
            invalid(format!(
                "dependency key {raw:?} is not a valid pin (expected OWNER/REPO@REF)"
            ))
        })?;
        let dependency = parse_dependency(schema, raw, &pin, body)?;
        // Two spellings of one pin (`actions/checkout` and `Actions/Checkout`) are one entry when
        // their metadata agrees and a conflict otherwise, as in the Go parser.
        if let Some(existing) = dependencies.get(&dependency.pin_key) {
            if *existing != dependency {
                return Err(invalid(format!(
                    "duplicate action key {:?} after canonicalization with differing metadata",
                    dependency.pin_key
                )));
            }
            continue;
        }
        dependencies.insert(dependency.pin_key.clone(), dependency);
    }
    Ok(dependencies)
}

fn parse_dependency(
    schema: Schema,
    raw_key: &str,
    pin: &PinParts,
    body: &Value,
) -> Result<LockedDependency, LockfileError> {
    let fields = action_fields(schema, raw_key, body)?;
    // Presence is checked in the order of the Go package, before any value is read.
    let ref_value = match schema {
        Schema::V001 => None,
        Schema::V002 | Schema::V003 => Some(required(&fields, "ref", raw_key)?),
    };
    let commit_value = required(&fields, "commit", raw_key)?;
    let owner_value = required(&fields, "owner_id", raw_key)?;
    let repo_value = required(&fields, "repo_id", raw_key)?;

    let hostname = if schema.dotcom_only() {
        Some(DOTCOM_HOST.to_owned())
    } else {
        fields
            .get("hostname")
            .copied()
            .map(|value| hostname_field(value, raw_key))
            .transpose()?
    };
    let commit = commit_field(commit_value, pin, schema, raw_key)?;
    let owner_id = id_field(owner_value, "owner_id", raw_key)?;
    let repo_id = id_field(repo_value, "repo_id", raw_key)?;
    let reference = match ref_value {
        Some(value) => ref_field(value, pin, raw_key)?,
        None => v001_reference(&fields, pin, raw_key)?,
    };
    let uses = match fields.get("uses").copied() {
        Some(value) => pin_list(value, schema, &format!("dependency {raw_key:?}"))?,
        None => Vec::new(),
    };
    Ok(LockedDependency {
        pin_key: pin.canonical(),
        reference,
        commit,
        owner_id,
        repo_id,
        hostname,
        uses,
    })
}

/// The fields of an action body. Names the schema does not define are rejected.
fn action_fields<'a>(
    schema: Schema,
    raw_key: &str,
    body: &'a Value,
) -> Result<Fields<'a>, LockfileError> {
    let mapping = body.as_mapping().ok_or_else(|| {
        invalid(format!(
            "action metadata for dependency {raw_key:?} must be a mapping"
        ))
    })?;
    let mut fields: Fields<'a> = BTreeMap::new();
    for (name, value) in mapping {
        let name = name.as_str().ok_or_else(|| {
            invalid(format!(
                "action field names for dependency {raw_key:?} must be strings"
            ))
        })?;
        if !schema.allows_field(name) {
            return Err(invalid(format!(
                "unknown action field {name:?} for dependency {raw_key:?}"
            )));
        }
        fields.insert(name, value);
    }
    Ok(fields)
}

/// The reference of a v0.0.1 entry: its `tag`, else its `branch`, else the ref in its pin key.
fn v001_reference(
    fields: &Fields<'_>,
    pin: &PinParts,
    raw_key: &str,
) -> Result<String, LockfileError> {
    let tag = optional_string(fields, "tag", raw_key)?;
    let branch = optional_string(fields, "branch", raw_key)?;
    Ok(tag
        .into_iter()
        .chain(branch)
        .find(|text| !text.is_empty())
        .map_or_else(|| pin.reference.clone(), str::to_owned))
}

/// The value of a field the schema requires.
fn required<'a>(
    fields: &Fields<'a>,
    field: &str,
    raw_key: &str,
) -> Result<&'a Value, LockfileError> {
    fields.get(field).copied().ok_or_else(|| {
        invalid(format!(
            "missing required action field {field:?} for dependency {raw_key:?}"
        ))
    })
}

fn optional_string<'a>(
    fields: &Fields<'a>,
    field: &str,
    raw_key: &str,
) -> Result<Option<&'a str>, LockfileError> {
    match fields.get(field).copied() {
        Some(value) => string_field(value, field, raw_key).map(Some),
        None => Ok(None),
    }
}

fn string_field<'a>(
    value: &'a Value,
    field: &str,
    raw_key: &str,
) -> Result<&'a str, LockfileError> {
    value.as_str().ok_or_else(|| {
        invalid(format!(
            "action field {field:?} must be a string for dependency {raw_key:?}"
        ))
    })
}

/// A numeric ID: a positive integer that fits in `i64`, as the Go package's `int64` requires.
fn id_field(value: &Value, field: &str, raw_key: &str) -> Result<u64, LockfileError> {
    value
        .as_i64()
        .filter(|id| *id > 0)
        .and_then(|id| u64::try_from(id).ok())
        .ok_or_else(|| {
            invalid(format!(
                "action field {field:?} must be a positive integer for dependency {raw_key:?}"
            ))
        })
}

fn hostname_field(value: &Value, raw_key: &str) -> Result<String, LockfileError> {
    let text = string_field(value, "hostname", raw_key)?;
    if text.is_empty() {
        return Err(empty_field("hostname", raw_key));
    }
    if !is_canonical_hostname(text) {
        return Err(invalid(format!(
            "action field \"hostname\" must be \"github.com\" or a lowercase canonical GHE tenant hostname for dependency {raw_key:?}, got {text:?}"
        )));
    }
    Ok(text.to_owned())
}

/// `github.com`, or a single lowercase tenant label under `ghe.com`: 1 to 63 characters of
/// `a-z`, `0-9` and `-`, not starting or ending with `-`.
fn is_canonical_hostname(host: &str) -> bool {
    if host == DOTCOM_HOST {
        return true;
    }
    let Some(tenant) = host.strip_suffix(".ghe.com") else {
        return false;
    };
    (1..=63).contains(&tenant.len())
        && !tenant.starts_with('-')
        && !tenant.ends_with('-')
        && tenant
            .bytes()
            .all(|byte| byte.is_ascii_lowercase() || byte.is_ascii_digit() || byte == b'-')
}

fn ref_field(value: &Value, pin: &PinParts, raw_key: &str) -> Result<String, LockfileError> {
    let text = string_field(value, "ref", raw_key)?;
    if text.is_empty() {
        return Err(empty_field("ref", raw_key));
    }
    // A full-SHA key is immutable, so its body ref is the symbolic ref the commit was found
    // under and may differ from the key.
    if text != pin.reference.as_str() && !is_full_sha(&pin.reference) {
        return Err(invalid(format!(
            "action body ref {text:?} does not match pin key ref {:?} for dependency {raw_key:?}",
            pin.reference
        )));
    }
    Ok(text.to_owned())
}

fn commit_field(
    value: &Value,
    pin: &PinParts,
    schema: Schema,
    raw_key: &str,
) -> Result<LockedCommit, LockfileError> {
    let text = string_field(value, "commit", raw_key)?;
    if text.is_empty() {
        return Err(empty_field("commit", raw_key));
    }
    let commit = parse_commit(text).ok_or_else(|| {
        invalid(format!(
            "action field \"commit\" must be an algo-hex digest (e.g. \"sha1-abc...\") for dependency {raw_key:?}, got {text:?}"
        ))
    })?;
    if schema != Schema::V001
        && is_full_sha(&pin.reference)
        && commit.hex != pin.reference.to_ascii_lowercase()
    {
        return Err(invalid(format!(
            "pin key ref is a full SHA ({}) but commit digest {text:?} does not match for dependency {raw_key:?}",
            short_sha(&pin.reference)
        )));
    }
    Ok(commit)
}

/// Reads a list of pin keys (a workflow's pins or an action's `uses`), canonicalized. `owner`
/// names the referring entry in error messages.
fn pin_list(value: &Value, schema: Schema, owner: &str) -> Result<Vec<String>, LockfileError> {
    match value {
        Value::Null => Ok(Vec::new()),
        Value::Sequence(items) => items
            .iter()
            .map(|item| -> Result<String, LockfileError> {
                let Some(text) = item.as_str() else {
                    return Err(invalid(format!("{owner} lists a pin that is not a string")));
                };
                parse_pin_key(schema, text)
                    .map(|pin| pin.canonical())
                    .ok_or_else(|| {
                        invalid(format!(
                            "{owner} lists {text:?}, which is not a valid pin (expected OWNER/REPO@REF)"
                        ))
                    })
            })
            .collect(),
        _ => Err(invalid(format!("{owner} must list its pins as a sequence"))),
    }
}

/// Keeps the first occurrence of each pin, in file order. A workflow's pins are a set, so a
/// repeat adds nothing and would repeat per-job verification and job-message entries.
fn distinct_in_order(pins: Vec<String>) -> Vec<String> {
    pins.into_iter()
        .collect::<IndexSet<String>>()
        .into_iter()
        .collect()
}

fn parse_workflows(
    schema: Schema,
    entries: &Mapping,
) -> Result<BTreeMap<String, Vec<String>>, LockfileError> {
    let mut workflows: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (key, pins) in entries {
        let raw = key
            .as_str()
            .ok_or_else(|| invalid("workflow paths must be strings"))?;
        let path = normalize_workflow_path(raw).map_err(invalid)?;
        if workflows.contains_key(&path) {
            return Err(invalid(format!("duplicate workflow path {path:?}")));
        }
        let pins = distinct_in_order(pin_list(pins, schema, &format!("workflow {path:?}"))?);
        workflows.insert(path, pins);
    }
    Ok(workflows)
}

/// Checks that every pin a workflow or a `uses` list names has a dependency entry, and that the
/// `uses` graph is acyclic.
fn check_references(
    workflows: &BTreeMap<String, Vec<String>>,
    dependencies: &BTreeMap<String, LockedDependency>,
) -> Result<(), LockfileError> {
    for (path, pins) in workflows {
        if let Some(pin) = pins
            .iter()
            .find(|pin| !dependencies.contains_key(pin.as_str()))
        {
            return Err(LockfileError::DanglingPin {
                referrer: format!("workflow {path:?}"),
                pin_key: pin.clone(),
            });
        }
    }
    for dependency in dependencies.values() {
        if let Some(used) = dependency
            .uses
            .iter()
            .find(|used| !dependencies.contains_key(used.as_str()))
        {
            return Err(LockfileError::DanglingPin {
                referrer: format!("dependency {:?}", dependency.pin_key),
                pin_key: used.clone(),
            });
        }
    }
    match find_uses_cycle(dependencies) {
        Some(key) => Err(invalid(format!(
            "uses cycle detected at dependency {key:?}"
        ))),
        None => Ok(()),
    }
}

/// Depth-first search state for [`find_uses_cycle`].
#[derive(Clone, Copy, PartialEq, Eq)]
enum Colour {
    OnStack,
    Done,
}

/// A dependency on a `uses` cycle, if the graph has one. The search keeps its own stack, so a
/// long chain of `uses` cannot exhaust the call stack.
fn find_uses_cycle(dependencies: &BTreeMap<String, LockedDependency>) -> Option<&str> {
    // Keys absent from `colour` are unvisited.
    let mut colour: BTreeMap<&str, Colour> = BTreeMap::new();
    for start in dependencies.keys() {
        if colour.contains_key(start.as_str()) {
            continue;
        }
        colour.insert(start.as_str(), Colour::OnStack);
        let mut stack: Vec<(&str, usize)> = vec![(start.as_str(), 0)];
        while let Some(frame) = stack.last_mut() {
            let (key, next) = *frame;
            let uses: &[String] = match dependencies.get(key) {
                Some(dependency) => &dependency.uses,
                None => &[],
            };
            if let Some(child) = uses.get(next) {
                frame.1 += 1;
                match colour.get(child.as_str()) {
                    Some(Colour::OnStack) => return Some(child.as_str()),
                    Some(Colour::Done) => {}
                    None => {
                        colour.insert(child.as_str(), Colour::OnStack);
                        stack.push((child.as_str(), 0));
                    }
                }
            } else {
                colour.insert(key, Colour::Done);
                stack.pop();
            }
        }
    }
    None
}

/// Validates a workflow path key and returns its normalized form, or the reason it is unsafe.
///
/// Keys are consumed as repository file paths, so absolute paths, `..` segments, control
/// characters, backslashes and colons are rejected. Leading `./`, `.` and empty segments are
/// dropped. Case is kept, because file paths are case-sensitive on the runner.
fn normalize_workflow_path(raw: &str) -> Result<String, String> {
    if raw.is_empty() {
        return Err("workflow path key must not be empty".to_owned());
    }
    if raw.starts_with('/') {
        return Err(format!(
            "workflow path key must be repo-relative, not absolute: {raw:?}"
        ));
    }
    if raw.chars().any(is_control_char) {
        return Err(format!(
            "workflow path key contains control characters: {raw:?}"
        ));
    }
    if raw.contains(['\\', ':']) {
        return Err(format!(
            "workflow path key contains an invalid character: {raw:?}"
        ));
    }
    if raw.split('/').any(|segment| segment == "..") {
        return Err(format!(
            "workflow path key contains path traversal: {raw:?}"
        ));
    }
    let segments: Vec<&str> = raw
        .split('/')
        .filter(|segment| !segment.is_empty() && *segment != ".")
        .collect();
    if segments.is_empty() {
        return Err(format!("workflow path key does not name a file: {raw:?}"));
    }
    Ok(segments.join("/"))
}

/// A pin key's parts, with owner and repo lowercased.
#[derive(Debug, Clone, PartialEq, Eq)]
struct PinParts {
    owner: String,
    repo: String,
    reference: String,
}

impl PinParts {
    fn canonical(&self) -> String {
        format!("{}/{}@{}", self.owner, self.repo, self.reference)
    }
}

/// Builds pin parts from validated pieces, lowercasing owner and repo.
fn pin_parts(owner: &str, repo: &str, reference: &str) -> Option<PinParts> {
    if !is_valid_segment(owner) || !is_valid_segment(repo) || !is_valid_ref(reference) {
        return None;
    }
    Some(PinParts {
        owner: owner.to_ascii_lowercase(),
        repo: repo.to_ascii_lowercase(),
        reference: reference.to_owned(),
    })
}

/// Splits `owner/repo`, which must have exactly one slash.
fn split_repo(repo_path: &str) -> Option<(&str, &str)> {
    let (owner, repo) = repo_path.split_once('/')?;
    (!repo.contains('/')).then_some((owner, repo))
}

/// Parses `OWNER/REPO@REF`, the form of `workflows` entries and of v0.0.2 and v0.0.3 keys.
fn parse_pin(text: &str) -> Option<PinParts> {
    let (repo_path, reference) = text.split_once('@')?;
    let (owner, repo) = split_repo(repo_path)?;
    pin_parts(owner, repo, reference)
}

/// Parses a v0.0.1 pin, `OWNER/REPO@REF:ALGO-HEX`, dropping the digest suffix when it is one.
fn parse_pin_v001(text: &str) -> Option<PinParts> {
    let (repo_path, reference) = text.split_once('@')?;
    let reference = match reference.rsplit_once(':') {
        Some((ref_part, digest)) if !ref_part.is_empty() && looks_like_algo_hex(digest) => ref_part,
        _ => reference,
    };
    let (owner, repo) = split_repo(repo_path)?;
    pin_parts(owner, repo, reference)
}

fn parse_pin_key(schema: Schema, text: &str) -> Option<PinParts> {
    match schema {
        Schema::V001 => parse_pin_v001(text),
        Schema::V002 | Schema::V003 => parse_pin(text),
    }
}

/// Whether `text` is `sha1-` or `sha256-` followed by lowercase hex, as the Go parser's
/// `looksLikeAlgoHex` requires.
fn looks_like_algo_hex(text: &str) -> bool {
    match text.split_once('-') {
        Some((algorithm, hex)) => {
            matches!(algorithm, "sha1" | "sha256")
                && !hex.is_empty()
                && hex
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        }
        None => false,
    }
}

/// Parses `sha1-<40 hex>` or `sha256-<64 hex>`, lowercasing the digest.
fn parse_commit(text: &str) -> Option<LockedCommit> {
    let (algorithm, hex) = text.split_once('-')?;
    let algorithm = algorithm.to_ascii_lowercase();
    let hex = hex.to_ascii_lowercase();
    let digits = match algorithm.as_str() {
        "sha1" => 40,
        "sha256" => 64,
        _ => return None,
    };
    if hex.len() != digits || !hex.bytes().all(|byte| byte.is_ascii_hexdigit()) {
        return None;
    }
    Some(LockedCommit { algorithm, hex })
}

/// A GitHub owner or repository name: non-empty ASCII letters, digits, `-`, `_` and `.`, and not
/// `.` or `..`. These names become API path segments, so nothing else is allowed.
fn is_valid_segment(text: &str) -> bool {
    text != "."
        && text != ".."
        && !text.is_empty()
        && text
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
}

/// A ref: non-empty, without `:` (the legacy digest separator) or control characters. A ref may
/// contain `/` and `@`.
fn is_valid_ref(text: &str) -> bool {
    !text.is_empty() && !text.contains(':') && !text.chars().any(is_control_char)
}

/// A `uses:` subpath: one or more valid segments separated by `/`.
fn is_valid_subpath(text: &str) -> bool {
    !text.is_empty() && text.split('/').all(is_valid_segment)
}

/// Whether a `uses:` subpath names a file under `.github/workflows/`, which is a reusable workflow
/// rather than an action. Mirrors `isWorkflowFile` in `uses.go`.
fn is_reusable_workflow_file(subpath: &str) -> bool {
    subpath.starts_with(".github/workflows/")
        && (subpath.ends_with(".yml") || subpath.ends_with(".yaml"))
}

/// A full commit SHA: 40 (SHA-1) or 64 (SHA-256) hex digits, in either case.
fn is_full_sha(text: &str) -> bool {
    matches!(text.len(), 40 | 64) && text.bytes().all(|byte| byte.is_ascii_hexdigit())
}

fn short_sha(text: &str) -> &str {
    text.get(..12).unwrap_or(text)
}

/// ASCII control characters, which the Go package treats as invalid in refs and `uses:` values.
fn is_control_char(c: char) -> bool {
    c <= '\u{1f}' || c == '\u{7f}'
}

#[cfg(test)]
mod tests {
    use super::*;

    const CHECKOUT_SHA: &str = "34e114876b0b11c390a56381ad16ebd13914f8d5";
    const SETUP_GO_SHA: &str = "40f1582b2485089dde7abd97c1529aa768e1baff";
    const CACHE_SHA: &str = "2222222222222222222222222222222222222222";
    const DEPLOY_SHA: &str = "1111111111111111111111111111111111111111";
    const COMMIT_A: &str = "sha1-11bd71901bbe5b1630ceea73d27597364c9af683";
    const UNRELATED_SHA: &str = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa";

    const V3_FIXTURE: &str = r#"version: 'v0.0.3'
workflows:
  '.github/workflows/ci.yml':
    - 'actions/setup-go@v5.6.0'
    - 'actions/checkout@v4.3.1'
    - 'actions/cache@v4'
  '.github/workflows/deploy.yml':
    - 'octocorp/deploy@v1'
dependencies:
  'actions/checkout@v4.3.1':
    ref: 'v4.3.1'
    commit: 'sha1-34e114876b0b11c390a56381ad16ebd13914f8d5'
    owner_id: 44036562
    repo_id: 197814629
  'actions/setup-go@v5.6.0':
    ref: 'v5.6.0'
    commit: 'sha1-40f1582b2485089dde7abd97c1529aa768e1baff'
    owner_id: 44036562
    repo_id: 192624594
  'actions/cache@v4':
    hostname: 'github.com'
    ref: 'v4'
    commit: 'sha1-2222222222222222222222222222222222222222'
    owner_id: 44036562
    repo_id: 220101
    uses:
      - 'ACTIONS/Checkout@v4.3.1'
  'octocorp/deploy@v1':
    hostname: 'octocorp.ghe.com'
    ref: 'v1'
    commit: 'sha1-1111111111111111111111111111111111111111'
    owner_id: 11
    repo_id: 22
"#;

    const V2_FIXTURE: &str = r#"version: v0.0.2
workflows:
  .github/workflows/ci.yml:
    - actions/checkout@v4
dependencies:
  actions/checkout@v4:
    ref: v4
    commit: sha1-11bd71901bbe5b1630ceea73d27597364c9af683
    owner_id: 1
    repo_id: 2
"#;

    const V1_FIXTURE: &str = r#"version: 'v0.0.1'
workflows:
  '.github/workflows/ci.yml':
    - 'actions/checkout@v4:sha1-11bd71901bbe5b1630ceea73d27597364c9af683'
    - 'actions/composite@v1:sha1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
dependencies:
  'actions/checkout@v4:sha1-11bd71901bbe5b1630ceea73d27597364c9af683':
    tag: 'v4'
    branch: 'main'
    commit: 'sha1-11bd71901bbe5b1630ceea73d27597364c9af683'
    owner_id: 1
    repo_id: 2
  'actions/internal@trunk:sha1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa':
    branch: 'trunk'
    commit: 'sha1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    owner_id: 3
    repo_id: 4
  'actions/composite@v1:sha1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa':
    commit: 'sha1-aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa'
    owner_id: 5
    repo_id: 6
    uses:
      - 'actions/checkout@v4:sha1-11bd71901bbe5b1630ceea73d27597364c9af683'
"#;

    /// What `gh actions-lock` wrote for `github/actions-lockfile` itself: v0.0.2 with four-space
    /// indentation and the generator's comment header.
    const GH_ACTIONS_LOCK_V002: &str = r#"# This file is machine-generated by `gh actions-lock`.
# Do not edit by hand; run `gh actions-lock` to update.
# Docs: https://gh.io/actions-lockfile
version: 'v0.0.2'
workflows:
    '.github/workflows/ci.yml':
        - 'actions/checkout@v4.3.1'
        - 'actions/setup-go@v5.6.0'
    '.github/workflows/dependency-review.yml':
        - 'actions/checkout@v4.3.1'
        - 'actions/dependency-review-action@v4.9.0'
    '.github/workflows/release.yml':
        - 'actions/checkout@v6.0.2'
dependencies:
    'actions/checkout@v4.3.1':
        ref: 'v4.3.1'
        commit: 'sha1-34e114876b0b11c390a56381ad16ebd13914f8d5'
        owner_id: 44036562
        repo_id: 197814629
    'actions/checkout@v6.0.2':
        ref: 'v6.0.2'
        commit: 'sha1-de0fac2e4500dabe0009e67214ff5f5447ce83dd'
        owner_id: 44036562
        repo_id: 197814629
    'actions/dependency-review-action@v4.9.0':
        ref: 'v4.9.0'
        commit: 'sha1-2031cfc080254a8a887f58cffee85186f0e49e48'
        owner_id: 44036562
        repo_id: 476372928
    'actions/setup-go@v5.6.0':
        ref: 'v5.6.0'
        commit: 'sha1-40f1582b2485089dde7abd97c1529aa768e1baff'
        owner_id: 44036562
        repo_id: 192624594
"#;

    fn assert_invalid(source: &str, needle: &str) {
        match ActionsLock::parse(source) {
            Err(LockfileError::Invalid(message)) => {
                assert!(message.contains(needle), "{message:?} lacks {needle:?}");
            }
            other => panic!("expected an Invalid error containing {needle:?}, got {other:?}"),
        }
    }

    /// One v0.0.3 `dependencies` entry at ref `v1` with the given direct `uses`.
    fn entry(pin: &str, uses: &[&str]) -> String {
        let mut text = format!(
            "  '{pin}':\n    ref: 'v1'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n"
        );
        if !uses.is_empty() {
            text.push_str("    uses:\n");
            text.push_str(
                &uses
                    .iter()
                    .map(|used| format!("      - '{used}'\n"))
                    .collect::<String>(),
            );
        }
        text
    }

    fn lockfile(entries: &[String]) -> String {
        format!("version: 'v0.0.3'\ndependencies:\n{}", entries.concat())
    }

    fn workflow_doc(path: &str) -> String {
        format!("version: 'v0.0.3'\nworkflows:\n  '{path}': []\n")
    }

    fn hostname_doc(hostname: &str) -> String {
        format!(
            "version: 'v0.0.3'\ndependencies:\n  'example/action@v1':\n    hostname: '{hostname}'\n    ref: 'v1'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n"
        )
    }

    /// A v0.0.3 lockfile with `count` distinct dependencies, each committed to `commit`.
    fn dependencies_doc(count: usize, commit: &str) -> String {
        let entries: String = (0..count)
            .map(|index| {
                format!(
                    "  'example/action-{index}@v1':\n    ref: 'v1'\n    commit: '{commit}'\n    owner_id: 1\n    repo_id: 2\n"
                )
            })
            .collect();
        format!("version: 'v0.0.3'\ndependencies:\n{entries}")
    }

    #[test]
    fn v003_fixture_exposes_pins_hosts_and_identities() {
        let lock = ActionsLock::parse(V3_FIXTURE).expect("v0.0.3 fixture parses");
        assert_eq!(lock.version(), "v0.0.3");
        let ci = vec![
            "actions/setup-go@v5.6.0".to_owned(),
            "actions/checkout@v4.3.1".to_owned(),
            "actions/cache@v4".to_owned(),
        ];
        assert_eq!(
            lock.workflow_pins(".github/workflows/ci.yml"),
            Some(ci.as_slice())
        );
        let checkout = lock
            .dependency("actions/checkout@v4.3.1")
            .expect("checkout is locked");
        assert_eq!(checkout.reference, "v4.3.1");
        assert_eq!(checkout.hostname, None);
        assert_eq!(checkout.owner_id, 44_036_562);
        assert_eq!(checkout.repo_id, 197_814_629);
        assert_eq!(checkout.commit.algorithm, "sha1");
        assert_eq!(checkout.commit.as_git_sha(), CHECKOUT_SHA);
        let cache = lock
            .dependency("actions/cache@v4")
            .expect("cache is locked");
        assert_eq!(cache.hostname.as_deref(), Some("github.com"));
        assert_eq!(cache.uses, vec!["actions/checkout@v4.3.1".to_owned()]);
        let deploy = lock
            .dependency("octocorp/deploy@v1")
            .expect("deploy is locked");
        assert_eq!(deploy.hostname.as_deref(), Some("octocorp.ghe.com"));
    }

    #[test]
    fn wire_dependencies_keep_workflow_order_with_digests_and_explicit_hosts() {
        let lock = ActionsLock::parse(V3_FIXTURE).expect("v0.0.3 fixture parses");
        assert_eq!(
            lock.wire_dependencies(".github/workflows/ci.yml"),
            Some(vec![
                format!("actions/setup-go@v5.6.0:sha1-{SETUP_GO_SHA}"),
                format!("actions/checkout@v4.3.1:sha1-{CHECKOUT_SHA}"),
                format!("github.com/actions/cache@v4:sha1-{CACHE_SHA}"),
            ])
        );
        assert_eq!(
            lock.wire_dependencies(".github/workflows/deploy.yml"),
            Some(vec![format!(
                "octocorp.ghe.com/octocorp/deploy@v1:sha1-{DEPLOY_SHA}"
            )])
        );
    }

    #[test]
    fn onboarding_is_per_workflow_and_paths_are_normalized() {
        let lock = ActionsLock::parse(V3_FIXTURE).expect("v0.0.3 fixture parses");
        assert!(lock.is_onboarded(".github/workflows/ci.yml"));
        assert!(lock.is_onboarded("./.github/workflows/ci.yml"));
        assert!(!lock.is_onboarded(".github/workflows/other.yml"));
        assert!(!lock.is_onboarded("/.github/workflows/ci.yml"));
        assert!(!lock.is_onboarded(".github/workflows/CI.yml"));
        assert_eq!(lock.workflow_pins(".github/workflows/other.yml"), None);
        assert_eq!(lock.wire_dependencies(".github/workflows/other.yml"), None);
        assert_eq!(
            lock.wire_dependencies("./.github/workflows/ci.yml"),
            lock.wire_dependencies(".github/workflows/ci.yml")
        );
        assert_eq!(
            lock.workflow_paths().collect::<Vec<&str>>(),
            vec![".github/workflows/ci.yml", ".github/workflows/deploy.yml"]
        );
    }

    #[test]
    fn onboarded_workflows_without_pins_are_empty_not_absent() {
        let source = "version: 'v0.0.3'\nworkflows:\n  '.github/workflows/empty.yml': []\n  '.github/workflows/null.yml':\n";
        let lock = ActionsLock::parse(source).expect("parses");
        assert!(lock.is_onboarded(".github/workflows/empty.yml"));
        assert!(
            lock.workflow_pins(".github/workflows/null.yml")
                .is_some_and(<[String]>::is_empty)
        );
        assert_eq!(
            lock.wire_dependencies(".github/workflows/empty.yml"),
            Some(Vec::new())
        );
    }

    #[test]
    fn workflow_keys_are_normalized_and_collisions_are_rejected() {
        let lock = ActionsLock::parse(&workflow_doc("./.github/workflows/a.yml")).expect("parses");
        assert!(lock.is_onboarded(".github/workflows/a.yml"));
        let lock = ActionsLock::parse(&workflow_doc(".github/./workflows//b.yml")).expect("parses");
        assert!(lock.is_onboarded(".github/workflows/b.yml"));
        assert_invalid(
            "version: 'v0.0.3'\nworkflows:\n  '.github/workflows/a.yml': []\n  './.github/workflows/a.yml': []\n",
            "duplicate workflow path",
        );
    }

    #[test]
    fn unsafe_workflow_path_keys_are_rejected_and_repo_paths_accepted() {
        for key in [
            "../../../etc/passwd",
            ".github/../../../etc/passwd",
            "/etc/shadow",
            ".github/workflows/../../evil.yml",
            "C:/Windows/system.ini",
            r"..\..\secret",
            r"\\server\share",
            "",
        ] {
            assert_invalid(&workflow_doc(key), "workflow path key");
        }
        for key in [
            ".github/workflows/ci.yml",
            ".github/workflows/release.yaml",
            "custom/path/workflow.yml",
        ] {
            assert!(ActionsLock::parse(&workflow_doc(key)).is_ok(), "{key}");
        }
    }

    #[test]
    fn version_policy_separates_supported_future_and_unsupported_versions() {
        for version in ["v0.0.1", "v0.0.2", "v0.0.3"] {
            let lock = ActionsLock::parse(&format!("version: '{version}'\n"))
                .expect("supported version parses");
            assert_eq!(lock.version(), version);
        }
        for version in ["v0.0.4", "v0.0.999", "v9.0.0", "0.0.4"] {
            let err = ActionsLock::parse(&format!("version: '{version}'\n"))
                .expect_err("future version is rejected");
            match &err {
                LockfileError::FutureVersion { version: found, .. } => {
                    assert_eq!(found.as_str(), version);
                }
                other => panic!("expected FutureVersion for {version}, got {other}"),
            }
            assert!(err.to_string().contains("upgrade"), "{err}");
        }
        for version in ["garbage", "v0.0.0", "v0.0.3-beta", "V0.0.3", ""] {
            let err = ActionsLock::parse(&format!("version: '{version}'\n"))
                .expect_err("unsupported version is rejected");
            assert!(
                matches!(&err, LockfileError::UnsupportedVersion { .. }),
                "{err}"
            );
        }
    }

    #[test]
    fn blank_documents_and_documents_without_version_are_missing_a_version() {
        for source in ["", "  \n# only a comment\n", "dependencies: {}\n", "~\n"] {
            assert!(
                matches!(
                    ActionsLock::parse(source),
                    Err(LockfileError::MissingVersion)
                ),
                "{source:?}"
            );
        }
    }

    #[test]
    fn non_mapping_documents_and_non_string_versions_are_rejected() {
        assert_invalid("- a\n- b\n", "lockfile must be a YAML mapping");
        assert_invalid("version: 3\n", "must be a string");
        assert_invalid("version: [v0.0.3]\n", "must be a string");
        assert_invalid("version: 'v0.0.3'\nextra: 1\n", "unknown lockfile field");
        assert_invalid(
            "version: 'v0.0.3'\nworkflows: []\n",
            "workflows must be a mapping",
        );
        assert_invalid(
            "version: 'v0.0.3'\ndependencies: []\n",
            "dependencies must be a mapping",
        );
    }

    #[test]
    fn yaml_syntax_errors_are_malformed_with_a_location() {
        match ActionsLock::parse("version: [v0.0.3\n") {
            Err(LockfileError::Malformed { line, column, .. }) => {
                assert!(line.is_some(), "line");
                assert!(column.is_some(), "column");
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn duplicate_yaml_keys_are_malformed_with_a_location() {
        let source = format!(
            "version: 'v0.0.3'\ndependencies:\n{}{}",
            entry("example/action@v1", &[]),
            entry("example/action@v1", &[])
        );
        match ActionsLock::parse(&source) {
            Err(LockfileError::Malformed {
                message,
                line,
                column,
            }) => {
                assert!(message.contains("duplicate entry"), "{message}");
                assert!(line.is_some() && column.is_some(), "{message}");
            }
            other => panic!("expected Malformed, got {other:?}"),
        }
    }

    #[test]
    fn casing_variants_of_one_pin_merge_only_when_their_metadata_agrees() {
        let same = format!(
            "version: 'v0.0.3'\ndependencies:\n  'actions/checkout@v6':\n    ref: 'v6'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n  'Actions/Checkout@v6':\n    ref: 'v6'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n"
        );
        let lock = ActionsLock::parse(&same).expect("identical metadata merges");
        assert!(lock.dependency("actions/checkout@v6").is_some());
        let differing = format!(
            "version: 'v0.0.3'\ndependencies:\n  'actions/checkout@v6':\n    ref: 'v6'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n  'Actions/Checkout@v6':\n    ref: 'v6'\n    commit: '{COMMIT_A}'\n    owner_id: 9\n    repo_id: 2\n"
        );
        assert_invalid(&differing, "duplicate action key");
    }

    #[test]
    fn hostnames_must_be_github_com_or_a_canonical_ghe_tenant() {
        let tenant_63 = format!("{}.ghe.com", "a".repeat(63));
        for hostname in [
            "github.com",
            "octocorp.ghe.com",
            "a.ghe.com",
            "octo-corp2.ghe.com",
            tenant_63.as_str(),
        ] {
            assert!(
                ActionsLock::parse(&hostname_doc(hostname)).is_ok(),
                "{hostname} must be accepted"
            );
        }
        let tenant_64 = format!("{}.ghe.com", "a".repeat(64));
        for hostname in [
            "",
            "GitHub.com",
            "Octocorp.ghe.com",
            "https://octocorp.ghe.com",
            "octocorp.ghe.com:443",
            "octocorp.ghe.com/path",
            "github.com.evil.example",
            "octo.ghe.com.evil",
            "ghe.com",
            ".ghe.com",
            "-octo.ghe.com",
            "octo-.ghe.com",
            "octo_corp.ghe.com",
            tenant_64.as_str(),
        ] {
            assert!(
                ActionsLock::parse(&hostname_doc(hostname)).is_err(),
                "{hostname:?} must be rejected"
            );
        }
        let mutated =
            hostname_doc("octocorp.ghe.com").replace("hostname: 'octocorp.ghe.com'", "hostname: 5");
        assert_invalid(&mutated, "must be a string");
    }

    #[test]
    fn commits_must_be_sha1_or_sha256_algo_hex_digests() {
        let sha1 = "a".repeat(40);
        let sha256 = "b".repeat(64);
        for commit in [format!("sha1-{sha1}"), format!("sha256-{sha256}")] {
            let source = format!(
                "version: 'v0.0.3'\ndependencies:\n  'example/action@v1':\n    ref: 'v1'\n    commit: '{commit}'\n    owner_id: 1\n    repo_id: 2\n"
            );
            assert!(ActionsLock::parse(&source).is_ok(), "{commit}");
        }
        let invalid_commits = [
            String::new(),
            "notadigest".to_owned(),
            "sha1-".to_owned(),
            "sha1-abc123".to_owned(),
            "HEAD".to_owned(),
            "sha1".to_owned(),
            format!("sha1-{}", "z".repeat(40)),
            format!("md5-{}", "a".repeat(32)),
            format!("sha256-{sha1}"),
            format!("sha1-{sha256}"),
        ];
        for commit in invalid_commits {
            let source = format!(
                "version: 'v0.0.3'\ndependencies:\n  'example/action@v1':\n    ref: 'v1'\n    commit: '{commit}'\n    owner_id: 1\n    repo_id: 2\n"
            );
            assert_invalid(&source, "commit");
        }
    }

    #[test]
    fn numeric_ids_must_be_positive_integers() {
        for (owner, repo) in [
            ("0", "2"),
            ("1", "-2"),
            ("'12'", "2"),
            ("1.5", "2"),
            ("1", "0"),
        ] {
            let source = format!(
                "version: 'v0.0.3'\ndependencies:\n  'example/action@v1':\n    ref: 'v1'\n    commit: '{COMMIT_A}'\n    owner_id: {owner}\n    repo_id: {repo}\n"
            );
            assert_invalid(&source, "must be a positive integer");
        }
    }

    #[test]
    fn action_bodies_reject_unknown_missing_and_mistyped_fields() {
        let unknown = format!(
            "version: 'v0.0.3'\ndependencies:\n  'example/action@v1':\n    tag: 'v1'\n    ref: 'v1'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n"
        );
        assert_invalid(&unknown, "unknown action field \"tag\"");
        let missing = format!(
            "version: 'v0.0.3'\ndependencies:\n  'example/action@v1':\n    ref: 'v1'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n"
        );
        assert_invalid(&missing, "missing required action field \"repo_id\"");
        let not_mapping = "version: 'v0.0.3'\ndependencies:\n  'example/action@v1': 5\n";
        assert_invalid(not_mapping, "must be a mapping");
        let non_string_key = "version: 'v0.0.3'\ndependencies:\n  7: {}\n";
        assert_invalid(non_string_key, "dependency keys must be strings");
        let empty_ref = format!(
            "version: 'v0.0.3'\ndependencies:\n  'example/action@v1':\n    ref: ''\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n"
        );
        assert_invalid(&empty_ref, "must not be empty");
    }

    #[test]
    fn dependency_keys_must_be_repo_scoped_pins_without_digest_suffixes() {
        for key in [
            "not a pin",
            "actions/cache/save@v4",
            "owner/repo@v4:sha1-11bd71901bbe5b1630ceea73d27597364c9af683",
            "../evil@v1",
            "owner/repo@",
            "@v1",
            "owner/re po@v1",
        ] {
            let source = format!(
                "version: 'v0.0.3'\ndependencies:\n  '{key}':\n    ref: 'v4'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n"
            );
            assert_invalid(&source, "is not a valid pin");
        }
    }

    #[test]
    fn workflow_and_uses_entries_must_be_pins() {
        let workflow = "version: 'v0.0.3'\nworkflows:\n  '.github/workflows/ci.yml':\n    - 'actions/checkout@v4:sha1-11bd71901bbe5b1630ceea73d27597364c9af683'\n";
        assert_invalid(workflow, "is not a valid pin");
        let uses = format!(
            "version: 'v0.0.3'\ndependencies:\n  'actions/cache@v4':\n    ref: 'v4'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n    uses:\n      - 'actions/cache/save@v4'\n"
        );
        assert_invalid(&uses, "is not a valid pin");
    }

    #[test]
    fn workflow_and_uses_entries_need_dependency_entries() {
        let dangling_workflow = "version: 'v0.0.3'\nworkflows:\n  '.github/workflows/ci.yml':\n    - 'actions/checkout@v4'\n";
        match ActionsLock::parse(dangling_workflow) {
            Err(LockfileError::DanglingPin { pin_key, .. }) => {
                assert_eq!(pin_key, "actions/checkout@v4");
            }
            other => panic!("expected DanglingPin, got {other:?}"),
        }
        let dangling_uses = lockfile(&[entry("actions/a@v1", &["actions/missing@v1"])]);
        match ActionsLock::parse(&dangling_uses) {
            Err(LockfileError::DanglingPin { pin_key, .. }) => {
                assert_eq!(pin_key, "actions/missing@v1");
            }
            other => panic!("expected DanglingPin, got {other:?}"),
        }
    }

    #[test]
    fn uses_graph_must_be_acyclic_but_may_share_targets() {
        let cycle = lockfile(&[
            entry("actions/a@v1", &["actions/b@v1"]),
            entry("actions/b@v1", &["actions/a@v1"]),
        ]);
        assert_invalid(&cycle, "uses cycle");
        let self_loop = lockfile(&[entry("actions/a@v1", &["actions/a@v1"])]);
        assert_invalid(&self_loop, "uses cycle");
        let diamond = lockfile(&[
            entry("actions/a@v1", &["actions/b@v1", "actions/c@v1"]),
            entry("actions/b@v1", &["actions/c@v1"]),
            entry("actions/c@v1", &[]),
        ]);
        assert!(ActionsLock::parse(&diamond).is_ok());
    }

    #[test]
    fn symbolic_key_refs_must_match_the_body_ref_but_full_shas_need_not() {
        assert_invalid(
            &format!(
                "version: 'v0.0.3'\ndependencies:\n  'actions/checkout@v4':\n    ref: 'v3'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n"
            ),
            "does not match pin key ref",
        );
        assert_invalid(
            &format!(
                "version: 'v0.0.3'\ndependencies:\n  'actions/checkout@v4':\n    ref: 'v4:foo'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n"
            ),
            "does not match pin key ref",
        );
        let accepted = format!(
            "version: 'v0.0.3'\ndependencies:\n  'actions/checkout@{CHECKOUT_SHA}':\n    ref: 'main'\n    commit: 'sha1-{CHECKOUT_SHA}'\n    owner_id: 1\n    repo_id: 2\n"
        );
        let lock = ActionsLock::parse(&accepted).expect("full-SHA key with a branch ref parses");
        assert_eq!(
            lock.dependency(&format!("actions/checkout@{CHECKOUT_SHA}"))
                .map(|dependency| dependency.reference.as_str()),
            Some("main")
        );
        assert_invalid(
            &format!(
                "version: 'v0.0.3'\ndependencies:\n  'actions/checkout@{CHECKOUT_SHA}':\n    ref: 'main'\n    commit: 'sha1-{UNRELATED_SHA}'\n    owner_id: 1\n    repo_id: 2\n"
            ),
            "does not match",
        );
    }

    #[test]
    fn commit_digests_are_lowercased() {
        let upper = format!(
            "version: 'v0.0.3'\ndependencies:\n  'actions/checkout@v4':\n    ref: 'v4'\n    commit: 'SHA1-{}'\n    owner_id: 1\n    repo_id: 2\n",
            CHECKOUT_SHA.to_ascii_uppercase()
        );
        let lock = ActionsLock::parse(&upper).expect("upper-case digests are accepted");
        let commit = &lock
            .dependency("actions/checkout@v4")
            .expect("checkout is locked")
            .commit;
        assert_eq!(commit.algorithm, "sha1");
        assert_eq!(commit.as_git_sha(), CHECKOUT_SHA);
        assert_eq!(commit.to_string(), format!("sha1-{CHECKOUT_SHA}"));
    }

    #[test]
    fn dependency_lookup_ignores_owner_and_repo_case_but_not_ref_case() {
        let lock = ActionsLock::parse(V3_FIXTURE).expect("v0.0.3 fixture parses");
        assert!(lock.dependency("ACTIONS/Checkout@v4.3.1").is_some());
        assert!(lock.dependency("actions/checkout@V4.3.1").is_none());
        assert!(lock.dependency("actions/checkout").is_none());
        assert!(lock.dependency("actions/missing@v1").is_none());
    }

    #[test]
    fn v002_dependencies_default_to_github_com_and_reject_a_hostname_field() {
        let lock = ActionsLock::parse(V2_FIXTURE).expect("v0.0.2 fixture parses");
        assert_eq!(lock.version(), "v0.0.2");
        let checkout = lock
            .dependency("actions/checkout@v4")
            .expect("checkout is locked");
        assert_eq!(checkout.hostname.as_deref(), Some("github.com"));
        assert_eq!(checkout.reference, "v4");
        assert_eq!(
            lock.wire_dependencies(".github/workflows/ci.yml"),
            Some(vec![format!("github.com/actions/checkout@v4:{COMMIT_A}")])
        );
        assert_invalid(
            &format!(
                "version: v0.0.2\ndependencies:\n  actions/checkout@v4:\n    hostname: github.com\n    ref: v4\n    commit: {COMMIT_A}\n    owner_id: 1\n    repo_id: 2\n"
            ),
            "unknown action field \"hostname\"",
        );
    }

    #[test]
    fn v001_uses_tag_then_branch_then_pin_ref_and_digest_suffixed_keys() {
        let lock = ActionsLock::parse(V1_FIXTURE).expect("v0.0.1 fixture parses");
        assert_eq!(lock.version(), "v0.0.1");
        let ci = vec![
            "actions/checkout@v4".to_owned(),
            "actions/composite@v1".to_owned(),
        ];
        assert_eq!(
            lock.workflow_pins(".github/workflows/ci.yml"),
            Some(ci.as_slice())
        );
        let checkout = lock
            .dependency("actions/checkout@v4")
            .expect("checkout is locked");
        assert_eq!(checkout.reference, "v4", "tag wins over branch");
        assert_eq!(checkout.hostname.as_deref(), Some("github.com"));
        assert_eq!(
            lock.dependency("actions/internal@trunk")
                .map(|dependency| dependency.reference.as_str()),
            Some("trunk")
        );
        let composite = lock
            .dependency("actions/composite@v1")
            .expect("composite is locked");
        assert_eq!(composite.reference, "v1", "pin key ref is the fallback");
        assert_eq!(composite.uses, vec!["actions/checkout@v4".to_owned()]);
        assert_eq!(
            lock.wire_dependencies(".github/workflows/ci.yml"),
            Some(vec![
                format!("github.com/actions/checkout@v4:{COMMIT_A}"),
                format!("github.com/actions/composite@v1:sha1-{}", "a".repeat(40)),
            ])
        );
        assert_invalid(
            &format!(
                "version: 'v0.0.1'\ndependencies:\n  'actions/checkout@v4:sha1-{CHECKOUT_SHA}':\n    ref: 'v4'\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n"
            ),
            "unknown action field \"ref\"",
        );
    }

    #[test]
    fn pin_key_for_uses_follows_the_go_classification() {
        let cases = [
            ("actions/checkout@v4", Some("actions/checkout@v4")),
            ("Actions/Checkout@V4", Some("actions/checkout@V4")),
            ("  actions/checkout@v4\n", Some("actions/checkout@v4")),
            ("actions/cache/save@v4", Some("actions/cache@v4")),
            ("actions/cache/save/deep@v4", Some("actions/cache@v4")),
            ("owner/repo@release/v1", Some("owner/repo@release/v1")),
            ("owner/repo@feat@x", Some("owner/repo@feat@x")),
            ("owner/repo.name@v1", Some("owner/repo.name@v1")),
            ("octo/repo/.github/workflows/ci.yml@v1", None),
            ("octo/repo/.github/workflows/ci.yaml@main", None),
            ("./local-action", None),
            ("./.github/workflows/ci.yml", None),
            ("$/actions/foo", None),
            ("$/.github/workflows/reusable.yml@v1", None),
            ("docker://alpine:3.18", None),
            ("actions/checkout@${{ matrix.ref }}", None),
            ("${{ matrix.action }}", None),
            ("actions/checkout", None),
            ("actions/checkout@", None),
            ("@v4", None),
            (
                "actions/checkout@v4:sha1-11bd71901bbe5b1630ceea73d27597364c9af683",
                None,
            ),
            ("../evil@v1", None),
            ("owner/repo/a/../b@v1", None),
            ("owner/repo/@v1", None),
            ("actions\tcheckout@v4", None),
        ];
        for (uses, expected) in cases {
            assert_eq!(pin_key_for_uses(uses).as_deref(), expected, "uses {uses:?}");
        }
    }

    #[test]
    fn oversized_input_is_rejected_before_parsing_and_the_exact_limit_is_not() {
        let oversized = "x".repeat(MAX_LOCKFILE_BYTES + 1);
        assert!(matches!(
            ActionsLock::parse(&oversized),
            Err(LockfileError::TooLarge { bytes, max })
                if bytes == MAX_LOCKFILE_BYTES + 1 && max == MAX_LOCKFILE_BYTES
        ));
        let at_limit = " ".repeat(MAX_LOCKFILE_BYTES);
        assert!(matches!(
            ActionsLock::parse(&at_limit),
            Err(LockfileError::MissingVersion)
        ));
    }

    #[test]
    fn dependency_count_limit_is_exact_and_checked_before_entries() {
        let at_limit = ActionsLock::parse(&dependencies_doc(MAX_DEPENDENCIES, COMMIT_A))
            .expect("exactly the limit parses");
        let first = "example/action-0@v1";
        let last = format!("example/action-{}@v1", MAX_DEPENDENCIES - 1);
        assert!(at_limit.dependency(first).is_some());
        assert!(at_limit.dependency(&last).is_some());
        assert!(matches!(
            ActionsLock::parse(&dependencies_doc(MAX_DEPENDENCIES + 1, COMMIT_A)),
            Err(LockfileError::TooManyDependencies { count, max })
                if count == MAX_DEPENDENCIES + 1 && max == MAX_DEPENDENCIES
        ));
        // The count is checked first, so invalid entries over the limit still report it.
        assert!(matches!(
            ActionsLock::parse(&dependencies_doc(MAX_DEPENDENCIES + 1, "notadigest")),
            Err(LockfileError::TooManyDependencies { .. })
        ));
    }

    #[test]
    fn repeated_workflow_pins_collapse_to_their_first_occurrence() {
        // `Actions/Checkout@v1` is the same canonical pin as the first entry.
        let lock = ActionsLock::parse(
            r#"version: 'v0.0.3'
workflows:
  '.github/workflows/ci.yml':
    - 'actions/checkout@v1'
    - 'actions/setup-go@v1'
    - 'Actions/Checkout@v1'
    - 'actions/cache@v1'
    - 'actions/setup-go@v1'
dependencies:
  'actions/checkout@v1':
    ref: 'v1'
    commit: 'sha1-11bd71901bbe5b1630ceea73d27597364c9af683'
    owner_id: 1
    repo_id: 2
  'actions/setup-go@v1':
    ref: 'v1'
    commit: 'sha1-11bd71901bbe5b1630ceea73d27597364c9af683'
    owner_id: 1
    repo_id: 2
  'actions/cache@v1':
    ref: 'v1'
    commit: 'sha1-11bd71901bbe5b1630ceea73d27597364c9af683'
    owner_id: 1
    repo_id: 2
"#,
        )
        .expect("repeated pins are accepted");
        let ci = ".github/workflows/ci.yml";
        let expected = vec![
            "actions/checkout@v1".to_owned(),
            "actions/setup-go@v1".to_owned(),
            "actions/cache@v1".to_owned(),
        ];
        assert_eq!(lock.workflow_pins(ci), Some(expected.as_slice()));
        assert_eq!(
            lock.wire_dependencies(ci),
            Some(vec![
                format!("actions/checkout@v1:{COMMIT_A}"),
                format!("actions/setup-go@v1:{COMMIT_A}"),
                format!("actions/cache@v1:{COMMIT_A}"),
            ])
        );
    }

    #[test]
    fn malformed_shapes_are_rejected_without_panicking() {
        let cases = [
            "version: 'v0.0.3'\ndependencies:\n  'example/action@v1': 5\n",
            "version: 'v0.0.3'\ndependencies:\n  'example/action@v1':\n    ref: [1]\n",
            "version: 'v0.0.3'\ndependencies: []\n",
            "version: 'v0.0.3'\nworkflows: []\n",
            "version: 'v0.0.3'\nworkflows:\n  '.github/workflows/ci.yml': 7\n",
            "version: 'v0.0.3'\nworkflows:\n  '.github/workflows/ci.yml':\n    - 7\n",
            "version: 'v0.0.3'\nworkflows:\n  7: []\n",
            "version: 'v0.0.3'\ndependencies:\n  7: {}\n",
            "version: v0.0.3\n? [a]\n: b\n",
            "version: 'v0.0.3'\nextra: 1\n",
            "- a\n- b\n",
            "version: [v0.0.3\n",
        ];
        for source in cases {
            assert!(
                ActionsLock::parse(source).is_err(),
                "{source:?} must be rejected"
            );
        }
    }

    #[test]
    fn v001_keys_lose_only_a_well_formed_digest_suffix() {
        for key in [
            "ACTIONS/Checkout@v4:sha1-11bd71901bbe5b1630ceea73d27597364c9af683",
            "actions/checkout@v4:sha256-abcdef",
            "actions/checkout@v4",
        ] {
            let source = format!(
                "version: 'v0.0.1'\ndependencies:\n  '{key}':\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n"
            );
            let lock = ActionsLock::parse(&source).expect("v0.0.1 key parses");
            assert!(lock.dependency("actions/checkout@v4").is_some(), "{key}");
        }
        for key in [
            "actions/checkout@v4:notadigest",
            "actions/checkout@v4:sha1-ABCDEF",
            "actions/checkout/save@v4:sha1-11bd71901bbe5b1630ceea73d27597364c9af683",
        ] {
            let source = format!(
                "version: 'v0.0.1'\ndependencies:\n  '{key}':\n    commit: '{COMMIT_A}'\n    owner_id: 1\n    repo_id: 2\n"
            );
            assert_invalid(&source, "is not a valid pin");
        }
    }

    #[test]
    fn lockfile_written_by_gh_actions_lock_parses_with_its_onboarded_workflows() {
        const DEPENDENCY_REVIEW_SHA: &str = "2031cfc080254a8a887f58cffee85186f0e49e48";
        let lock = ActionsLock::parse(GH_ACTIONS_LOCK_V002).expect("gh actions-lock output parses");
        assert_eq!(lock.version(), "v0.0.2");
        assert!(lock.is_onboarded(".github/workflows/release.yml"));
        assert!(!lock.is_onboarded(".github/workflows/deploy.yml"));
        let review = lock
            .dependency("actions/dependency-review-action@v4.9.0")
            .expect("dependency-review-action is locked");
        assert_eq!((review.owner_id, review.repo_id), (44_036_562, 476_372_928));
        assert_eq!(
            lock.wire_dependencies(".github/workflows/dependency-review.yml"),
            Some(vec![
                format!("github.com/actions/checkout@v4.3.1:sha1-{CHECKOUT_SHA}"),
                format!(
                    "github.com/actions/dependency-review-action@v4.9.0:sha1-{DEPENDENCY_REVIEW_SHA}"
                ),
            ])
        );
    }
}
