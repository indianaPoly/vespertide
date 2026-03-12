use sea_orm::{ConnectionTrait, DatabaseBackend, DatabaseConnection, Statement, TransactionTrait};

use crate::MigrationError;

#[derive(Debug, Clone, Copy)]
pub struct EmbeddedMigration {
    pub version: u32,
    pub migration_id: &'static str,
    pub comment: &'static str,
    pub postgres_sql_blob: &'static str,
    pub mysql_sql_blob: &'static str,
    pub sqlite_sql_blob: &'static str,
}

impl EmbeddedMigration {
    pub const fn new(
        version: u32,
        migration_id: &'static str,
        comment: &'static str,
        postgres_sql_blob: &'static str,
        mysql_sql_blob: &'static str,
        sqlite_sql_blob: &'static str,
    ) -> Self {
        Self {
            version,
            migration_id,
            comment,
            postgres_sql_blob,
            mysql_sql_blob,
            sqlite_sql_blob,
        }
    }

    pub const fn sql_blob(self, backend: DatabaseBackend) -> &'static str {
        match backend {
            DatabaseBackend::Postgres => self.postgres_sql_blob,
            DatabaseBackend::MySql => self.mysql_sql_blob,
            DatabaseBackend::Sqlite => self.sqlite_sql_blob,
            _ => self.postgres_sql_blob,
        }
    }
}

pub fn split_sql_blob(blob: &str) -> impl Iterator<Item = &str> {
    blob.split_terminator('\0').filter(|sql| !sql.is_empty())
}

pub async fn run_embedded_migrations(
    pool: &DatabaseConnection,
    version_table: &str,
    verbose: bool,
    migrations: &[EmbeddedMigration],
) -> Result<(), MigrationError> {
    let backend = pool.get_database_backend();
    let q = if matches!(backend, DatabaseBackend::MySql) {
        '`'
    } else {
        '"'
    };

    let create_table_sql = format!(
        "CREATE TABLE IF NOT EXISTS {q}{}{q} (version INTEGER PRIMARY KEY, id TEXT DEFAULT '', created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP)",
        version_table
    );
    let stmt = Statement::from_string(backend, create_table_sql);
    pool.execute_raw(stmt).await.map_err(|e| {
        MigrationError::DatabaseError(format!("Failed to create version table: {}", e))
    })?;

    let alter_sql = format!(
        "ALTER TABLE {q}{}{q} ADD COLUMN id TEXT DEFAULT ''",
        version_table
    );
    let stmt = Statement::from_string(backend, alter_sql);
    let _ = pool.execute_raw(stmt).await;

    let txn = pool.begin().await.map_err(|e| {
        MigrationError::DatabaseError(format!("Failed to begin transaction: {}", e))
    })?;

    let select_sql = format!(
        "SELECT MAX(version) as version FROM {q}{}{q}",
        version_table
    );
    let stmt = Statement::from_string(backend, select_sql);
    let version_result = txn
        .query_one_raw(stmt)
        .await
        .map_err(|e| MigrationError::DatabaseError(format!("Failed to read version: {}", e)))?;
    let version = version_result
        .and_then(|row| row.try_get::<i32>("", "version").ok())
        .unwrap_or(0) as u32;

    let select_ids_sql = format!("SELECT version, id FROM {q}{}{q}", version_table);
    let stmt = Statement::from_string(backend, select_ids_sql);
    let id_rows = txn
        .query_all_raw(stmt)
        .await
        .map_err(|e| MigrationError::DatabaseError(format!("Failed to read version ids: {}", e)))?;
    let mut version_ids = std::collections::HashMap::<u32, String>::new();
    for row in &id_rows {
        if let Ok(found_version) = row.try_get::<i32>("", "version") {
            let id = row.try_get::<String>("", "id").unwrap_or_default();
            version_ids.insert(found_version as u32, id);
        }
    }

    if verbose {
        eprintln!("[vespertide] Current database version: {}", version);
    }

    for migration in migrations {
        if version >= migration.version {
            continue;
        }

        if let Some(db_id) = version_ids.get(&migration.version)
            && !migration.migration_id.is_empty()
            && !db_id.is_empty()
            && db_id != migration.migration_id
        {
            return Err(MigrationError::IdMismatch {
                version: migration.version,
                expected: migration.migration_id.to_string(),
                found: db_id.clone(),
            });
        }

        if verbose {
            eprintln!(
                "[vespertide] Applying migration v{} ({})",
                migration.version, migration.comment
            );
        }

        let sql_blob = migration.sql_blob(backend);
        let sqls: Vec<_> = split_sql_blob(sql_blob).collect();

        for (sql_idx, sql) in sqls.iter().enumerate() {
            if verbose {
                eprintln!("[vespertide]   [{}/{}] {}", sql_idx + 1, sqls.len(), sql);
            }

            let stmt = Statement::from_string(backend, (*sql).to_owned());
            txn.execute_raw(stmt).await.map_err(|e| {
                MigrationError::DatabaseError(format!("Failed to execute SQL '{}': {}", sql, e))
            })?;
        }

        let insert_sql = format!(
            "INSERT INTO {q}{}{q} (version, id) VALUES ({}, '{}')",
            version_table, migration.version, migration.migration_id
        );
        let stmt = Statement::from_string(backend, insert_sql);
        txn.execute_raw(stmt).await.map_err(|e| {
            MigrationError::DatabaseError(format!("Failed to insert version: {}", e))
        })?;

        if verbose {
            eprintln!(
                "[vespertide] Migration v{} applied successfully",
                migration.version
            );
        }
    }

    txn.commit().await.map_err(|e| {
        MigrationError::DatabaseError(format!("Failed to commit transaction: {}", e))
    })?;

    Ok(())
}

#[cfg(test)]
mod tests {
    use sea_orm::DatabaseBackend;

    use super::{EmbeddedMigration, split_sql_blob};

    #[test]
    fn split_sql_blob_ignores_empty_segments() {
        let sqls: Vec<_> =
            split_sql_blob("CREATE TABLE users ();\0\0ALTER TABLE users;\0").collect();

        assert_eq!(sqls, vec!["CREATE TABLE users ();", "ALTER TABLE users;"]);
    }

    #[test]
    fn embedded_migration_selects_backend_blob() {
        let migration = EmbeddedMigration::new(1, "id", "comment", "pg\0", "mysql\0", "sqlite\0");

        assert_eq!(migration.sql_blob(DatabaseBackend::Postgres), "pg\0");
        assert_eq!(migration.sql_blob(DatabaseBackend::MySql), "mysql\0");
        assert_eq!(migration.sql_blob(DatabaseBackend::Sqlite), "sqlite\0");
    }
}
