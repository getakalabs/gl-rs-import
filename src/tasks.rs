use calamine::{open_workbook_auto_from_rs, Reader, Sheets};
use celery::prelude::TaskError;
use dotenv::dotenv;
use reqwest::{Client, Response};
use serde::Serialize;
use std::io::Cursor;

const CATEGORIES_SHEET_NAME: &str = "TO TRIM DOWN_Categories_v2_Tier";

#[derive(Serialize, Debug, Default, Clone)]
struct Category {
    name: String,
    level: usize,
    parent: Option<Box<Category>>,
}

impl Category {
    pub fn new(name: &str, tier: usize, parent: Option<Box<Category>>) -> Self {
        Self {
            name: name.to_string(),
            level: tier + 1,
            parent,
        }
    }
}

#[celery::task]
pub async fn IMPORT_CATEGORIES(filename: String) {
    let file_bytes = download_file(&filename).await?;

    let cursor = Cursor::new(&file_bytes);
    let workbook = match open_workbook_auto_from_rs(cursor) {
        Ok(workbook) => workbook,
        Err(err) => {
            return Err(TaskError::UnexpectedError(format!(
                "Failed to open workbook: {}",
                err
            )))
        }
    };

    let _categories = create_category_list(workbook)?;
}

async fn download_file(filename: &str) -> Result<Vec<u8>, TaskError> {
    dotenv().ok();

    let url = format!(
        "{}/files/{filename}",
        std::env::var("API_ADDR").unwrap_or_else(|_| "127.0.0.1:8081".into())
    );

    let client = Client::new();

    let response = match client.get(url).send().await {
        Ok(response) => response,
        Err(err) => {
            return Err(TaskError::UnexpectedError(format!(
                "Failed to send HTTP request: {}",
                err
            )))
        }
    };

    if !response.status().is_success() {
        return Err(TaskError::UnexpectedError(format!(
            "Request failed: {}",
            response.status()
        )));
    }

    check_filetype(&response)?;

    let response_bytes = match response.bytes().await {
        Ok(bytes) => bytes.to_vec(),
        Err(err) => {
            return Err(TaskError::UnexpectedError(format!(
                "Failed to get bytes from response: {}",
                err
            )))
        }
    };

    Ok(response_bytes)
}

fn check_filetype(response: &Response) -> Result<(), TaskError> {
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

    Ok(())
}

fn create_category_list(
    mut workbook: Sheets<Cursor<&Vec<u8>>>,
) -> Result<Vec<Category>, TaskError> {
    let sheet = match workbook.worksheet_range(CATEGORIES_SHEET_NAME) {
        Some(sheet) => sheet,
        None => {
            return Err(TaskError::UnexpectedError(
                "Failed to get sheet".to_string(),
            ))
        }
    };
    let range = match sheet {
        Ok(range) => range,
        Err(err) => {
            return Err(TaskError::UnexpectedError(format!(
                "Failed to get sheet range: {}",
                err
            )))
        }
    };
    let rows = range
        .rows()
        .skip(1) // skip header
        .map(|r| {
            let cells: Vec<&str> = r
                .iter()
                .map(|c| c.get_string().unwrap_or_default())
                .collect();
            (cells[0], cells)
        });

    let result = parse_rows(rows);

    Ok(result)
}

fn parse_rows<'a, I>(rows: I) -> Vec<Category>
where
    I: Iterator<Item = (&'a str, Vec<&'a str>)>,
{
    let mut categories = Vec::new();
    let mut parent_stack: Vec<Box<Category>> = Vec::new();

    for (_, cells) in rows {
        let mut name = "";
        let mut level = 0;
        for (i, cell) in cells.into_iter().enumerate() {
            if !cell.is_empty() {
                name = cell;
                level = i;
                break;
            }
        }

        while !parent_stack.is_empty() && parent_stack.last().unwrap().level > level {
            parent_stack.pop();
        }

        let parent = parent_stack.last().cloned();
        let category = Category::new(name, level, parent);
        categories.push(category.clone());
        parent_stack.push(Box::new(category));
    }

    categories
}

fn _check_duplicates() {
    todo!("Check for existing records in DB prior to saving to prevent duplicates")
}
