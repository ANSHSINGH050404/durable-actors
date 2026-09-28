use std::panic::AssertUnwindSafe;

use futures_util::FutureExt;

use super::*;
use testing::{TestDatabase, with_postgres, with_postgres_schema};

#[tokio::test]
async fn fixtures_isolate_migrations_data_and_reconnections() -> Result<()> {
    with_postgres(async |first| {
        with_postgres(async |second| {
            let first_client = first.pool.get().await?;
            let another_client = first.pool.get().await?;
            let second_client = second.pool.get().await?;
            let first_schema = current_schema(&first_client).await?;
            let second_schema = current_schema(&second_client).await?;
            assert_eq!(first_schema, current_schema(&another_client).await?);
            assert_ne!(first_schema, second_schema);
            check_isolated_rows(&first_client, &second_client).await?;
            check_isolated_migrations(&first_client, &second_client).await?;
            check_reconnected_fixture(&second.url, &second_schema).await
        })
        .await
    })
    .await
}

#[tokio::test]
async fn fixtures_cleanup_after_success_error_and_panic() -> Result<()> {
    with_postgres(async |observer| {
        let client = observer.pool.get().await?;
        for outcome in ["success", "error", "panic"] {
            check_fixture_cleanup(&client, outcome).await?;
        }
        Ok(())
    })
    .await
}

#[test]
fn embedded_migrations_have_no_version_gaps() {
    let runner = embedded::migrations::runner();
    let mut versions: Vec<_> = runner
        .get_migrations()
        .iter()
        .map(|migration| migration.version())
        .collect();
    versions.sort_unstable();
    let expected: Vec<_> = (1..=versions.len() as i32).collect();
    assert_eq!(versions, expected);
}

#[tokio::test]
async fn contract_schema_keeps_one_contract_per_project() -> Result<()> {
    with_postgres(async |database| {
        let client = database.pool.get().await?;
        client
            .batch_execute(
                "INSERT INTO durable_actors_deployment (project_id, image_ref, working_directory)
             VALUES ('team-a', 'image', '/app'), ('team-b', 'image', '/app');
             INSERT INTO durable_actors_contracts (project_id, contract_hash, contract_json)
             VALUES ('team-a', 'hash', '{}'), ('team-b', 'hash', '{}');",
            )
            .await?;
        let duplicate = client
            .execute(
                "INSERT INTO durable_actors_contracts (project_id, contract_hash, contract_json)
             VALUES ('team-a', 'other-hash', '{}')",
                &[],
            )
            .await
            .unwrap_err();
        assert_eq!(
            duplicate.code(),
            Some(&tokio_postgres::error::SqlState::UNIQUE_VIOLATION)
        );
        client
            .execute(
                "DELETE FROM durable_actors_deployment WHERE project_id = 'team-a'",
                &[],
            )
            .await?;
        let project: String = client
            .query_one("SELECT project_id FROM durable_actors_contracts", &[])
            .await?
            .get(0);
        assert_eq!(project, "team-b");
        Ok(())
    })
    .await
}

#[tokio::test]
async fn concurrent_connections_migrate_fresh_and_existing_schemas_once() -> Result<()> {
    for version in [0, 2] {
        with_postgres_schema(async |database| check_concurrent_migrations(database, version).await)
            .await?;
    }
    Ok(())
}

#[test]
fn runtime_startup_does_not_connect_to_postgres() -> Result<()> {
    let database = PostgresDatabase::lazy("postgresql://localhost:1/unavailable?sslmode=disable")?;
    assert_eq!(database.pool.status().size, 0);
    Ok(())
}

#[tokio::test]
async fn independent_queries_can_use_different_database_connections() -> Result<()> {
    with_postgres(async |fixture| {
        let database = PostgresDatabase::connect(&fixture.url).await?;
        let (first, second) = tokio::try_join!(
            database.query_one("SELECT pg_backend_pid(), pg_sleep(0.05)", &[]),
            database.query_one("SELECT pg_backend_pid(), pg_sleep(0.05)", &[]),
        )?;
        assert_ne!(first.get::<_, i32>(0), second.get::<_, i32>(0));
        Ok(())
    })
    .await
}

async fn current_schema(client: &tokio_postgres::Client) -> Result<String> {
    Ok(client
        .query_one("SELECT current_schema()", &[])
        .await?
        .get(0))
}

async fn check_isolated_rows(
    first: &tokio_postgres::Client,
    second: &tokio_postgres::Client,
) -> Result<()> {
    let insert = "INSERT INTO durable_actors_deployment (project_id, image_ref, working_directory) VALUES ('project', 'image', '/app')";
    first.execute(insert, &[]).await?;
    let count: i64 = second
        .query_one("SELECT count(*) FROM durable_actors_deployment", &[])
        .await?
        .get(0);
    assert_eq!(count, 0);
    second.execute(insert, &[]).await?;
    Ok(())
}

async fn check_isolated_migrations(
    first: &tokio_postgres::Client,
    second: &tokio_postgres::Client,
) -> Result<()> {
    first
        .execute("DELETE FROM refinery_schema_history WHERE version = 3", &[])
        .await?;
    let count: i64 = second
        .query_one(
            "SELECT count(*) FROM refinery_schema_history WHERE version = 3",
            &[],
        )
        .await?
        .get(0);
    assert_eq!(count, 1);
    Ok(())
}

async fn check_reconnected_fixture(url: &str, expected_schema: &str) -> Result<()> {
    let reopened = PostgresDatabase::connect(url).await?;
    let client = reopened.connection().await?;
    assert_eq!(current_schema(&client).await?, expected_schema);
    let count: i64 = client
        .query_one("SELECT count(*) FROM durable_actors_deployment", &[])
        .await?
        .get(0);
    assert_eq!(count, 1);
    Ok(())
}

async fn check_fixture_cleanup(client: &tokio_postgres::Client, outcome: &str) -> Result<()> {
    let mut schema = String::new();
    let result = AssertUnwindSafe(with_postgres(async |fixture| {
        schema = current_schema(&*fixture.pool.get().await?).await?;
        match outcome {
            "error" => anyhow::bail!("test failure"),
            "panic" => panic!("test panic"),
            _ => Ok(()),
        }
    }))
    .catch_unwind()
    .await;
    match outcome {
        "error" => assert_eq!(result.unwrap().unwrap_err().to_string(), "test failure"),
        "panic" => assert!(result.is_err()),
        _ => result.unwrap()?,
    }
    let exists: bool = client
        .query_one(
            "SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname = $1)",
            &[&schema],
        )
        .await?
        .get(0);
    assert!(!exists, "fixture schema remained after {outcome}");
    Ok(())
}

async fn check_concurrent_migrations(database: &TestDatabase, version: i32) -> Result<()> {
    let mut client = database.pool.get().await?;
    if version > 0 {
        embedded::migrations::runner()
            .set_target(refinery::Target::Version(version))
            .run_async(&mut **client)
            .await?;
    }
    check_concurrent_connections(&database.url).await?;
    let versions: Vec<i32> = client
        .query(
            "SELECT version FROM refinery_schema_history ORDER BY version",
            &[],
        )
        .await?
        .iter()
        .map(|row| row.get(0))
        .collect();
    assert_eq!(versions, vec![1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11]);
    Ok(())
}

async fn check_concurrent_connections(url: &str) -> Result<()> {
    let barrier = std::sync::Arc::new(tokio::sync::Barrier::new(8));
    let mut tasks = tokio::task::JoinSet::new();
    for _ in 0..8 {
        let url = url.to_owned();
        let barrier = barrier.clone();
        tasks.spawn(async move {
            barrier.wait().await;
            PostgresDatabase::connect(&url).await
        });
    }
    let mut failures = Vec::new();
    while let Some(result) = tasks.join_next().await {
        if let Err(error) = result? {
            failures.push(format!("{error:#}"));
        }
    }
    assert!(
        failures.is_empty(),
        "concurrent migrations failed: {failures:#?}"
    );
    Ok(())
}
