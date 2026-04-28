use anyhow::{Context, Result};
use serde::{Deserialize, Serialize};
use std::fs;
use std::path::{Path, PathBuf};
use std::time::{SystemTime, UNIX_EPOCH};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct RelayProgressState {
    pub last_submitted_height: u64,
    pub last_submitted_hash: String,
    pub updated_at_unix_secs: u64,
}

impl RelayProgressState {
    pub fn new(last_submitted_height: u64, last_submitted_hash: String) -> Self {
        Self {
            last_submitted_height,
            last_submitted_hash,
            updated_at_unix_secs: current_unix_secs(),
        }
    }
}

#[derive(Debug, Clone)]
pub struct JsonFileStateStore {
    path: PathBuf,
}

impl JsonFileStateStore {
    pub fn new(path: impl Into<PathBuf>) -> Self {
        Self { path: path.into() }
    }

    pub fn load(&self) -> Result<Option<RelayProgressState>> {
        if !self.path.exists() {
            return Ok(None);
        }

        let content = fs::read_to_string(&self.path)
            .with_context(|| format!("failed reading state file {}", self.path.display()))?;
        let state: RelayProgressState = serde_json::from_str(content.as_str())
            .with_context(|| format!("failed parsing state file {}", self.path.display()))?;
        Ok(Some(state))
    }

    pub fn save(&self, state: &RelayProgressState) -> Result<()> {
        ensure_parent_dir(&self.path)?;

        let tmp_path = self.path.with_extension("tmp");
        let content = serde_json::to_string_pretty(state).context("failed to serialize state json")?;
        fs::write(&tmp_path, content)
            .with_context(|| format!("failed writing temp state file {}", tmp_path.display()))?;
        fs::rename(&tmp_path, &self.path).with_context(|| {
            format!(
                "failed replacing state file {} with {}",
                self.path.display(),
                tmp_path.display()
            )
        })?;
        Ok(())
    }
}

fn ensure_parent_dir(path: &Path) -> Result<()> {
    if let Some(parent) = path.parent() {
        fs::create_dir_all(parent)
            .with_context(|| format!("failed creating state directory {}", parent.display()))?;
    }
    Ok(())
}

fn current_unix_secs() -> u64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_secs())
        .unwrap_or(0)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::env;

    #[test]
    fn json_store_roundtrip_load_save() {
        let mut path = env::temp_dir();
        path.push(format!("btc-relay-state-test-{}.json", std::process::id()));

        let store = JsonFileStateStore::new(&path);
        let state = RelayProgressState::new(123, "abcd".to_string());
        store.save(&state).expect("save");
        let loaded = store.load().expect("load").expect("state exists");
        assert_eq!(loaded.last_submitted_height, 123);
        assert_eq!(loaded.last_submitted_hash, "abcd");

        let _ = fs::remove_file(path);
    }
}
