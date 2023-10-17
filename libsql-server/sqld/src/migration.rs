use std::fs::read_to_string;
use std::path::Path;

use anyhow::Context;
use semver::Version as SemVer;

enum Version {
    Pre0_18,
    Named(SemVer),
}

pub fn maybe_migrate(db_path: &Path) -> anyhow::Result<()> {
    // migration is performed in steps, until the most current version is reached
    loop {
        match detect_version(db_path)? {
            Version::Pre0_18 => migrate_step_from_pre_0_18(db_path)?,
            // most recent version was reached: exit
            Version::Named(_) => return Ok(()),
        }
    }
}

fn detect_version(db_path: &Path) -> anyhow::Result<Version> {
    let version_file_path = db_path.join(".version");
    if !version_file_path.try_exists()? {
        return Ok(Version::Pre0_18);
    }

    let version_str = read_to_string(version_file_path)?;
    let version = SemVer::parse(&version_str).context("invalid version file")?;

    Ok(Version::Named(version))
}

fn migrate_step_from_pre_0_18(db_path: &Path) -> anyhow::Result<()> {
    tracing::info!("version < 0.18.0 detected, performing migration");

    fn try_migrate(db_path: &Path) -> anyhow::Result<()> {
        std::fs::write(db_path.join(".version"), b"0.18.0")?;
        let ns_dir = db_path.join("dbs").join("default");
        std::fs::create_dir_all(&ns_dir)?;

        let maybe_link = |name| -> anyhow::Result<()> {
            if db_path.join(name).try_exists()? {
                std::fs::hard_link(db_path.join(name), ns_dir.join(name))?;
            }

            Ok(())
        };

        // link standalone files
        maybe_link("data")?;
        maybe_link("data-shm")?;
        maybe_link("data-wal")?;
        maybe_link("wallog")?;
        maybe_link("client_wal_index")?;

        // link snapshots
        let snapshot_dir = db_path.join("snapshots");
        if snapshot_dir.exists() {
            let new_snap_dir = ns_dir.join("snapshots");
            std::fs::create_dir_all(&new_snap_dir)?;
            for entry in std::fs::read_dir(snapshot_dir)? {
                let entry = entry?;
                if let Some(name) = entry.path().file_name() {
                    std::fs::hard_link(entry.path(), new_snap_dir.join(name))?;
                }
            }
        }

        Ok(())
    }

    if let Err(e) = try_migrate(db_path) {
        let _ = std::fs::remove_dir_all(db_path.join("dbs"));
        return Err(e);
    }

    // best effort cleanup
    let try_remove = |name| {
        let path = db_path.join(name);
        if let Err(e) = std::fs::remove_file(&path) {
            tracing::warn!(
                "failed to remove stale file `{}` during migration: {e}",
                path.display()
            );
        }
    };

    try_remove("data");
    try_remove("data-shm");
    try_remove("data-wal");
    try_remove("wallog");
    try_remove("client_wal_index");

    Ok(())
}
