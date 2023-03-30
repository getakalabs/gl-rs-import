mod tasks;

use celery::{export::Arc, prelude::CeleryError, Celery};
use dotenv::dotenv;
use tasks::IMPORT_CATEGORIES;

pub async fn new() -> Result<Arc<Celery>, CeleryError> {
    dotenv().ok();
    celery::app!(
        broker = AMQPBroker { std::env::var("AMQP_ADDR").unwrap_or_else(|_| "amqp://127.0.0.1:5672".into()) },
        tasks = [IMPORT_CATEGORIES],
        task_routes = ["*" => "imports"],
        prefetch_count = 2,
        heartbeat = Some(10),
    ).await
}

pub async fn import_categories(filename: String) -> Result<(), CeleryError> {
    let app = self::new().await?;

    app.send_task(IMPORT_CATEGORIES::new(filename)).await?;

    Ok(())
}
