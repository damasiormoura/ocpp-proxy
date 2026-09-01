//! Durable storage for the retained charge point snapshot.
//!
//! The snapshot lives in memory in the MQTT publisher, which means a proxy
//! restart — a deploy, an LXC reboot — used to lose it. The first message the
//! charger sent afterwards republished the retained topic with every field the
//! restart had cleared, so Home Assistant's previous-session rows went
//! `unavailable` until another session happened to end. For a car charged twice
//! a week that is days of blank rows.
//!
//! Writing the snapshot to a file and reading it back at startup closes that.
//! The alternative — subscribing to our own retained topic and seeding from it
//! — needs no disk, but the seed races the charger's first message: the charger
//! reconnects within a few seconds of the proxy starting, and if its
//! StatusNotification is folded in first, the republish clobbers the retained
//! topic before the seed arrives. A file is read before the event loop starts,
//! so there is no race to lose. It also survives a broker that has lost its own
//! retained store.
//!
//! Nothing here is allowed to be fatal. The proxy is the charger's only path to
//! Mobi.e, and a read-only filesystem or a corrupt file must cost visibility in
//! Home Assistant, never charging. Every failure is a warning and an empty map.

use std::collections::HashMap;
use std::io::Write;
use std::path::{Path, PathBuf};

use tracing::{debug, info, warn};

use crate::mqtt::ChargePointState;

/// The on-disk map, keyed by Charge Point ID.
pub type SnapshotMap = HashMap<String, ChargePointState>;

/// Reads and writes the snapshot map to a JSON file.
///
/// A `None` path disables persistence entirely, which is what the tests and any
/// deployment without a writable state directory use.
#[derive(Debug, Clone, Default)]
pub struct SnapshotStore {
    path: Option<PathBuf>,
}

impl SnapshotStore {
    /// A store backed by `path`, or a no-op store when `path` is `None`.
    pub fn new(path: Option<PathBuf>) -> Self {
        Self { path }
    }

    /// A store that never touches the filesystem.
    pub fn disabled() -> Self {
        Self { path: None }
    }

    /// Whether this store actually persists anything.
    pub fn is_enabled(&self) -> bool {
        self.path.is_some()
    }

    /// The configured path, for logging.
    pub fn path(&self) -> Option<&Path> {
        self.path.as_deref()
    }

    /// Read the map back.
    ///
    /// A missing file is the normal first-boot case and is not worth a warning.
    /// A corrupt one is: it means something wrote garbage, and silently
    /// starting empty would hide that.
    pub fn load(&self) -> SnapshotMap {
        let Some(path) = &self.path else {
            return SnapshotMap::new();
        };

        let raw = match std::fs::read(path) {
            Ok(raw) => raw,
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                debug!(
                    component = "snapshot_store",
                    path = %path.display(),
                    "No stored snapshot yet"
                );
                return SnapshotMap::new();
            }
            Err(e) => {
                warn!(
                    component = "snapshot_store",
                    path = %path.display(),
                    error = %e,
                    "Could not read stored snapshot; starting empty"
                );
                return SnapshotMap::new();
            }
        };

        match serde_json::from_slice::<SnapshotMap>(&raw) {
            Ok(map) => {
                info!(
                    component = "snapshot_store",
                    path = %path.display(),
                    charge_points = map.len(),
                    "Restored charge point snapshot"
                );
                map
            }
            Err(e) => {
                warn!(
                    component = "snapshot_store",
                    path = %path.display(),
                    error = %e,
                    "Stored snapshot is not valid JSON; starting empty"
                );
                SnapshotMap::new()
            }
        }
    }

    /// Write the map out, atomically.
    ///
    /// Via a temporary file in the same directory plus a rename, so a crash or
    /// a full disk mid-write leaves the previous good file rather than a
    /// truncated one that the next `load` would reject.
    ///
    /// Synchronous on purpose. This runs on the MQTT publisher's own thread,
    /// which is separate from the tasks forwarding OCPP frames, so a blocking
    /// write here cannot delay charger traffic. The map is a few hundred bytes
    /// and is written only when a snapshot actually changes — a handful of
    /// times per charging session, not per MeterValues.
    pub fn save(&self, map: &SnapshotMap) {
        let Some(path) = &self.path else {
            return;
        };

        if let Err(e) = self.save_inner(path, map) {
            warn!(
                component = "snapshot_store",
                path = %path.display(),
                error = %e,
                "Could not persist charge point snapshot; it will not survive a restart"
            );
        }
    }

    fn save_inner(&self, path: &Path, map: &SnapshotMap) -> std::io::Result<()> {
        if let Some(dir) = path.parent() {
            if !dir.as_os_str().is_empty() {
                std::fs::create_dir_all(dir)?;
            }
        }

        let bytes = serde_json::to_vec(map)
            .map_err(|e| std::io::Error::new(std::io::ErrorKind::InvalidData, e))?;

        let tmp = path.with_extension("json.tmp");
        {
            let mut f = std::fs::File::create(&tmp)?;
            f.write_all(&bytes)?;
            // Without this the rename can land while the contents are still in
            // the page cache, which a power cut turns into an empty file.
            f.sync_all()?;
        }
        std::fs::rename(&tmp, path)?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::models::OcppMessageType;

    fn call(action: &str) -> OcppMessageType {
        OcppMessageType::Call {
            action: action.to_string(),
        }
    }

    /// A snapshot with a completed session in it, as after a real charge.
    fn snapshot_after_a_session() -> ChargePointState {
        let mut st = ChargePointState::default();
        st.apply(
            "StatusNotification",
            &call("StatusNotification"),
            r#"[2,"1","StatusNotification",{"connectorId":1,"errorCode":"NoError","status":"Available"}]"#,
        );
        st.apply(
            "StartTransaction",
            &call("StartTransaction"),
            r#"[2,"2","StartTransaction",{"connectorId":1,"idTag":"7264b25e","meterStart":8076445}]"#,
        );
        st.apply(
            "StopTransaction",
            &call("StopTransaction"),
            r#"[2,"3","StopTransaction",{"meterStop":8084000,"transactionId":1788214378,"reason":"EVDisconnected"}]"#,
        );
        st
    }

    fn temp_path(name: &str) -> PathBuf {
        let mut p = std::env::temp_dir();
        p.push(format!(
            "ocpp-snapshot-test-{}-{}",
            std::process::id(),
            name
        ));
        p.push("state.json");
        p
    }

    /// The whole point: what a session recorded comes back after a restart.
    #[test]
    fn round_trips_a_completed_session() {
        let path = temp_path("round-trip");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        let store = SnapshotStore::new(Some(path.clone()));

        let mut map = SnapshotMap::new();
        map.insert("MOBI-ALM-00058".to_string(), snapshot_after_a_session());
        store.save(&map);

        // A fresh store, as a restarted process would build.
        let restored = SnapshotStore::new(Some(path.clone())).load();
        assert_eq!(restored, map);

        let cp = &restored["MOBI-ALM-00058"];
        assert_eq!(cp.last_meter_stop_wh, Some(8084000));
        assert_eq!(cp.last_session_energy_wh, Some(8084000 - 8076445));
        assert_eq!(cp.last_transaction_id, Some(1788214378));
        assert_eq!(cp.last_stop_reason.as_deref(), Some("EVDisconnected"));

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// First boot. Not an error, and must not log one.
    #[test]
    fn missing_file_loads_empty() {
        let path = temp_path("missing");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        assert!(SnapshotStore::new(Some(path)).load().is_empty());
    }

    /// A corrupt file must not stop the proxy starting.
    #[test]
    fn corrupt_file_loads_empty() {
        let path = temp_path("corrupt");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        std::fs::create_dir_all(path.parent().unwrap()).unwrap();
        std::fs::write(&path, b"{not json").unwrap();

        assert!(SnapshotStore::new(Some(path.clone())).load().is_empty());
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// An unwritable path must be a warning, not a panic — the proxy is the
    /// charger's only route to Mobi.e.
    #[test]
    fn unwritable_path_does_not_panic() {
        // /proc is present and not writable in every environment this runs in.
        let store = SnapshotStore::new(Some(PathBuf::from("/proc/ocpp-proxy/state.json")));
        let mut map = SnapshotMap::new();
        map.insert("CP".to_string(), snapshot_after_a_session());
        store.save(&map);
        assert!(store.load().is_empty());
    }

    /// The disabled store is a no-op in both directions.
    #[test]
    fn disabled_store_persists_nothing() {
        let store = SnapshotStore::disabled();
        assert!(!store.is_enabled());
        let mut map = SnapshotMap::new();
        map.insert("CP".to_string(), snapshot_after_a_session());
        store.save(&map);
        assert!(store.load().is_empty());
    }

    /// Saving twice must leave one good file, not a stray temporary.
    #[test]
    fn repeated_saves_leave_no_temp_file() {
        let path = temp_path("resave");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        let store = SnapshotStore::new(Some(path.clone()));

        let mut map = SnapshotMap::new();
        map.insert("CP".to_string(), snapshot_after_a_session());
        store.save(&map);
        store.save(&map);

        let entries: Vec<_> = std::fs::read_dir(path.parent().unwrap())
            .unwrap()
            .map(|e| e.unwrap().file_name().to_string_lossy().to_string())
            .collect();
        assert_eq!(entries, vec!["state.json".to_string()]);

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }

    /// The directory is created on demand: systemd's StateDirectory makes it,
    /// but a hand-run proxy or a changed path should not need a mkdir first.
    #[test]
    fn creates_the_directory() {
        let path = temp_path("mkdir");
        let _ = std::fs::remove_dir_all(path.parent().unwrap());
        assert!(!path.parent().unwrap().exists());

        let store = SnapshotStore::new(Some(path.clone()));
        store.save(&SnapshotMap::new());
        assert!(path.exists());

        let _ = std::fs::remove_dir_all(path.parent().unwrap());
    }
}
