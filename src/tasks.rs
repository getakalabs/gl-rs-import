use calamine::{open_workbook_auto, DataType, Reader};
use celery::prelude::TaskError;
use dotenv::dotenv;
use reqwest::blocking::Client;
use std::fs::File;

#[celery::task]
pub async fn IMPORT_CATEGORIES(filename: String) {
    dotenv().ok();

    println!("Downloading file");
    let url = format!(
        "{}/files/{filename}",
        std::env::var("API_ADDR").unwrap_or_else(|_| "localhost:8081".into())
    );

    // create a new HTTP client
    println!("Creating client");
    let client = Client::new();

    // send a GET request to the URL
    let response = match client.get(url).send() {
        Ok(response) => response,
        Err(err) => {
            return Err(TaskError::UnexpectedError(format!(
                "Failed to send HTTP request: {}",
                err
            )))
        }
    };

    // check if the request was successful
    if !response.status().is_success() {
        return Err(TaskError::UnexpectedError(format!(
            "request failed: {}",
            response.status()
        )));
    }

    // check if the response is an Excel file
    let content_type = match response.headers().get("Content-Type") {
        Some(header) => header.to_str().unwrap_or(""),
        None => {
            return Err(TaskError::UnexpectedError(
                "Missing content type header".into(),
            ));
        }
    };
    if !content_type.contains("application/vnd.openxmlformats-officedocument.spreadsheetml.sheet") {
        return Err(TaskError::UnexpectedError("Not an Excel file".into()));
    }

    // create a new file to write the downloaded data
    let file = match File::create(filename) {
        Ok(file) => file,
        Err(err) => {
            return Err(TaskError::UnexpectedError(format!(
                "Failed to create file: {}",
                err
            )));
        }
    };
}
