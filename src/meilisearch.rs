use crate::actions::MeilisearchDocument;
use serde::Deserialize;
use serde_json::{json};
use reqwest::{ StatusCode, Error, Url, header::*, Response };

use std::{iter::IntoIterator, fmt::Write};

use crate::actions::{Engine, CsvDocument};
#[derive(Deserialize)]
struct Index
{
    uid: String,
}

pub enum Query<'a> {
    ListIndexes,
    GetIndex(&'a str),

    GetDocument(&'a str, &'a str), // index + doc id
    GetDocumentsBatch(&'a str, usize, usize), // index + docs offset + docs limit
    AddOrReplaceDocuments(&'a str, &'a [CsvDocument]), // index + doc
    AddOrUpdateDocuments(&'a str, &'a [CsvDocument]), // index + doc
    DeleteDocument(&'a str, &'a str), // index + doc id
    DeleteDocuments(&'a str, &'a [&'a str]), // index + docs ids

    SearchDocument(&'a str, &'a str, usize), // index + query + limit
}

pub enum StressTestQuery<'a> {
    ListIndexes,
    GetRndIndex(&'a [&'a str]),

    //GetRndDocument(&'a)
}

pub struct Proxy { 
    pub base_url: Url,
    indexes_resourse: Url,
}

impl Proxy {

    pub fn new(base_url: Url) -> Self {
        let indexes_resourse = (&base_url).join("indexes/").unwrap();
        Self {
            base_url,
            indexes_resourse,
        }
    }

    pub async fn purge(&self) -> Result<String, Error> {
        let indexes : Vec<Index> = reqwest::get(self.indexes_resourse.as_str()).await?.json().await?;
        let client = reqwest::Client::new();
        let mut log = String::new();
        for index in indexes.iter() {
            let index_resourse = self.indexes_resourse.join(&index.uid).unwrap();
            match client.delete(index_resourse.as_str()).send().await {
                Ok(response) => {
                    match response.status() {
                        StatusCode::NO_CONTENT => writeln!(log, "Deleted index: {}", index_resourse),
                        StatusCode::NOT_FOUND => writeln!(log, "Cannot find index: {}", index_resourse),
                        status_code => writeln!(log, "Unknown response {:?} from index: {}", status_code, index_resourse)
                    };
                },
                Err(e) => {
                    writeln!(log, "Failed to send request to index {}", index_resourse);
                },
            };
        };
        Ok( log )
    }

    pub async fn stress_test(&self) {
        self.add_index("game", "myid").await;
    }

    pub async fn add_index(&self, name: &str, primary_key_name: &str) -> Result<String, Error> {
        let response = reqwest::Client::new()
            .post(self.indexes_resourse.as_str())
            .json(&json![{"uid": name, "primaryKey": primary_key_name}])
            .send()
            .await;
        Self::handle_response(response, "ADD_INDEX", StatusCode::CREATED)
            .await
    }

    pub async fn delete_index(&self, name: &str) -> Result<String, Error> {
        let index_resourse = self.indexes_resourse.join(name).unwrap();
        let response = reqwest::Client::new()
            .delete(index_resourse)
            .send()
            .await;
        Self::handle_response(response, "DELETE_INDEX", StatusCode::NO_CONTENT)
            .await
    }

    pub async fn list_indexes(&self) -> Result<String, Error> {
        let response = reqwest::get(self.indexes_resourse.as_str())
            .await;
        Self::handle_response(response, "LIST_INDEXES", StatusCode::OK)
            .await
        //println!("LIST_INDEXES: {:#?}", indexes_result);
    }

    pub async fn get_index(&self, name: &str) -> Result<String, Error> {
        let index_resourse = self.indexes_resourse.join(name).unwrap();
        let response = reqwest::get(index_resourse)
            .await;
        Self::handle_response(response, "GET_INDEX", StatusCode::OK)
            .await
    }

    pub async fn search_document(&self, index: &str, query: &str, limit: usize) -> Result<String, Error> {
        let index_resourse = self.indexes_resourse.join(index).unwrap();
        let response = reqwest::Client::new()
            .get(index_resourse)
            .query(&[("q", query), ("limit", &limit.to_string())])
            .send()
            .await;
        Self::handle_response(response, "SEARCH_DOC", StatusCode::OK)
            .await
    }

    pub async fn get_document(&self, index: &str, primary_key_val: &str) -> Result<String, Error> {
        let doc_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap()
            .join(primary_key_val).unwrap();
        let response = reqwest::get(doc_resourse)
            .await;
        Self::handle_response(response, "GET_DOC", StatusCode::OK)
            .await
    }

    pub async fn get_documents_batch(&self, index: &str, offset: usize, limit: usize) -> Result<String, Error> {
        let docs_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap();
        let response = reqwest::Client::new()
            .get(docs_resourse)
            .query(&[("offset", &offset.to_string()), ("limit", &limit.to_string())])
            .send()
            .await;
        Self::handle_response(response, "GET_DOCS_BATCH", StatusCode::OK)
            .await
    }

    pub async fn add_or_replace_documents<'a, I>(&self, index: &str, docs: I) -> Result<String, Error>
    where I: IntoIterator<Item=MeilisearchDocument> {
        let docs_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap();
        let response = reqwest::Client::new()
            .post(docs_resourse)
            .json(&crate::actions::SerializeIterator(docs.into_iter()))
            .send()
            .await;
        Self::handle_response(response, "ADD_OR_REPLACE_DOCS", StatusCode::ACCEPTED)
            .await
    }

    pub async fn add_or_update_documents(&self, index: &str, docs: &[CsvDocument]) -> Result<String, Error> {
        let docs_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap();
        let response = reqwest::Client::new()
            .put(docs_resourse)
            .json(&crate::actions::SerializeIterator(docs.into_iter()))
            .send()
            .await;
        Self::handle_response(response, "ADD_OR_UPDATE_DOCS", StatusCode::ACCEPTED)
            .await
    }

    pub async fn delete_document(&self, index: &str, primary_key_val: &str) -> Result<String, Error> {
        let doc_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap()
            .join(primary_key_val).unwrap();
        let response = reqwest::Client::new()
            .delete(doc_resourse)
            .send()
            .await;
        Self::handle_response(response, "DELETE_DOC", StatusCode::ACCEPTED)
            .await
    }

    pub async fn delete_documents(&self, index: &str, docs_ids: &[&str]) -> Result<String, Error> {
        let docs_batch_resourse = self.indexes_resourse
            .join(&format!("{}/documents/delete-batch", index)).unwrap();
        let response = reqwest::Client::new()
            .post(docs_batch_resourse)
            .json(docs_ids)
            .send()
            .await;
        Self::handle_response(response, "DELETE_DOCS", StatusCode::ACCEPTED)
            .await
    }
    
    async fn handle_response(reqwest_result: Result<Response, Error>, message_prefix: &str, expected_status: StatusCode) -> Result<String, Error> {
        match reqwest_result {
            Err(e) => { 
                println!("{} HTTP error: {:#?}", message_prefix, e);
                Err(e)
            },
            Ok(response) => {
                let is_unexpected_status = response.status() != expected_status;
                if is_unexpected_status {
                    println!("{} HTTP response code is not successful: {:#?}", message_prefix, response);    
                }
                let body = response.text().await;
                body
            },
        }
    }
}

#[tokio::test]
async fn test_meilisearch_actions() {
    let url = Url::parse("http://localhost:7700").unwrap();
    let engine = Engine::Meilisearch(url.clone());
    assert!(crate::actions::ping(engine).await.is_ok());

    let proxy = Proxy::new(url);
    assert!(proxy.purge().await.is_ok());

    assert!(proxy.add_index("test_idx", "test_id").await.is_ok());
    assert!(proxy.add_index("some_idx", "id").await.is_ok());

    assert!(proxy.list_indexes().await.is_ok());
    assert!(proxy.delete_index("test_idx").await.is_ok());

    assert!(proxy.get_index("some_idx").await.is_ok());

    let mut test_data_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_data_path.push("resourses/data.csv");
    let test_data = crate::actions::load_test_data(&test_data_path);
    assert!(test_data.is_ok(), "No test data file nearby");
    let test_data = test_data.unwrap();

    let documents_to_add = test_data.iter()
        .take(20)
        .enumerate()
        .map(|(idx, doc)| MeilisearchDocument { id: (idx + 1).to_string(), doc: doc.clone()});

    let documents_to_update = test_data.iter()
        .enumerate()
        .skip(10)
        .take(20)
        .map(|(idx, doc)| MeilisearchDocument { id: (idx + 1).to_string(), doc: doc.clone()});
    assert!(proxy.add_or_replace_documents("some_idx", documents_to_add).await.is_ok());
    std::thread::sleep(std::time::Duration::from_secs(1));
    assert!(proxy.search_document("some_idx", "rice", 10).await.is_ok());
    std::thread::sleep(std::time::Duration::from_secs(1));
    assert!(proxy.get_document("some_idx", "10").await.is_ok());

    assert!(proxy.add_or_update_documents("some_idx", documents_to_add).await.is_ok());
}
