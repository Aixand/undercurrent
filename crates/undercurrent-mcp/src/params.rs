// SPDX-License-Identifier: Apache-2.0
//! Assistant-facing inputs, independent of private runtime DTOs.
use schemars::JsonSchema;
use serde::Deserialize;
use std::collections::BTreeMap;

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Empty {}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct List {
    pub name: Option<String>,
    #[serde(default)]
    pub ids: Vec<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
    #[serde(default = "page_size")]
    #[schemars(range(min = 1, max = 100))]
    pub page_size: u32,
    /// Opaque cursor from this adapter connection; keep all filters unchanged.
    pub cursor: Option<String>,
}
fn page_size() -> u32 {
    20
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Inspect {
    pub session_id: String,
    #[serde(default)]
    pub process: bool,
    #[serde(default)]
    pub foreground_evidence: bool,
    #[serde(default)]
    pub health_results: bool,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Start {
    /// Unique name recorded before launching, for recovery after response loss.
    pub name: String,
    /// Executable name or path. Shell expressions require an explicit shell.
    pub program: String,
    #[serde(default)]
    pub arguments: Vec<String>,
    /// Absolute working directory on the MCP server's machine.
    pub cwd: String,
    #[serde(default)]
    pub environment: BTreeMap<String, String>,
    pub profile: Option<String>,
    #[serde(default)]
    pub labels: BTreeMap<String, String>,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Output {
    pub session_id: String,
    #[serde(default)]
    pub from_offset: u64,
    #[serde(default = "output_bytes")]
    #[schemars(range(min = 1, max = 32768))]
    pub max_bytes: u32,
    /// Include original byte arrays for lossless capture, including split UTF-8.
    #[serde(default)]
    pub include_bytes: bool,
}
fn output_bytes() -> u32 {
    8192
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Screen {
    pub session_id: String,
    #[serde(default = "output_bytes")]
    #[schemars(range(min = 1, max = 32768))]
    pub max_bytes: u32,
}

#[derive(Deserialize, JsonSchema)]
#[serde(rename_all = "snake_case")]
pub enum Key {
    Enter,
    Tab,
    Escape,
    Backspace,
    Delete,
    Up,
    Down,
    Left,
    Right,
    Home,
    End,
    PageUp,
    PageDown,
    CtrlC,
    CtrlD,
}

#[derive(Deserialize, JsonSchema)]
#[serde(tag = "kind", rename_all = "snake_case", deny_unknown_fields)]
pub enum Input {
    /// Literal text without an implicit Enter.
    Text {
        text: String,
    },
    /// Bracketed paste without an implicit Enter.
    Paste {
        text: String,
    },
    Key {
        key: Key,
    },
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Send {
    pub session_id: String,
    /// Caller-owned identity recorded with the exact input before delivery.
    pub delivery_id: String,
    pub expected_revision: Option<u64>,
    #[schemars(length(min = 1, max = 64))]
    pub items: Vec<Input>,
}

#[derive(Deserialize, JsonSchema, Default)]
#[serde(rename_all = "snake_case")]
pub enum StopMode {
    #[default]
    GracefulOnly,
    GracefulThenForce,
    Force,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Stop {
    pub session_id: String,
    pub expected_revision: Option<u64>,
    /// Windows requires explicit graceful_then_force or force; graceful_only is rejected before submission.
    #[serde(default)]
    pub mode: StopMode,
}

#[derive(Deserialize, JsonSchema)]
#[serde(deny_unknown_fields)]
pub struct Wait {
    pub session_id: String,
    /// OR within each axis, AND across axes. At least one axis is required.
    #[serde(default)]
    pub lifecycle: Vec<Lifecycle>,
    #[serde(default)]
    pub execution: Vec<Execution>,
    #[serde(default)]
    pub interaction: Vec<Interaction>,
    #[serde(default)]
    pub health: Vec<Health>,
    /// Inclusive revision floor; does not prove input acceptance or task success.
    pub min_revision: Option<u64>,
    #[serde(default = "wait_seconds")]
    #[schemars(range(min = 1, max = 60))]
    pub timeout_seconds: u64,
}
fn wait_seconds() -> u64 {
    30
}

macro_rules! states {
    ($name:ident { $($variant:ident),* }) => {
        #[derive(Deserialize, serde::Serialize, JsonSchema)]
        #[serde(rename_all = "snake_case")]
        pub enum $name { $($variant),* }
    };
}
states!(Lifecycle {
    Starting,
    Running,
    Stopping,
    Stopped,
    Exited,
    Failed
});
states!(Execution {
    Active,
    Quiescent,
    Unknown
});
states!(Interaction {
    None,
    Available,
    Requested
});
states!(Health {
    Healthy,
    Unhealthy,
    Unknown,
    NoCheck
});
