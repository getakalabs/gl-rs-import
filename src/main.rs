use celery::prelude::*;

#[tokio::main]
async fn main() -> Result<(), CeleryError> {
    let worker = importer::new().await?;

    println!("Worker start");
    worker.consume_from(&["imports"]).await?;
    worker.close().await?;
    println!("Worker stop");
    Ok(())
}
