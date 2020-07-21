
use crate::data::*;
use serde::Deserialize;
use serde_json::{json};
use reqwest::{StatusCode, Url, Response};
use rand::seq::{SliceRandom, IteratorRandom};
use rand::Rng;
use futures::prelude::*;
use futures::future::{join_all, ok, err};
use tokio::time::Instant;
use itertools::Itertools;
use async_std::{prelude::*, eprintln, eprint, print, println};

use std::{iter::IntoIterator, fmt::Write};
use std::sync::Arc;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Deref;

#[derive(Deserialize)]
struct Index
{
    pub uid: String,
}

pub struct StressTestResult {
    pub all_queries_send_time_ms: u128,
    pub all_queries_receive_time_ms: u128,
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

    async fn make_random_query<'a, I, N>(&self, index_name: N, doc_idx_range: (usize, usize), docs: I, search_queries: &[&'a str]) -> Result<String, Box<dyn Error>>
    where I: IntoIterator<Item=Document<'a>>, I::IntoIter: Clone, N: Deref, N::Target: AsRef<str> {
        let mut rng = rand::thread_rng();
        let random_doc_idx = rng.gen_range(doc_idx_range.0, doc_idx_range.1);
        let random_limit = rng.gen_range(1, 50);
        assert!(search_queries.len() > 0);
        let random_query = search_queries.choose(&mut rng).unwrap();
        let index_name = (*index_name).as_ref();
        let random_documents = docs.into_iter().choose_multiple(&mut rng, random_limit);
        match rng.gen_range(0_u32, 8) {
            0 => self.get_indexes().await,
            1 => self.get_document(index_name, &random_doc_idx.to_string()).await,
            2 => self.get_documents_batch(index_name, random_doc_idx, random_limit).await,
            3 => self.add_or_update_documents(index_name, random_documents).await,
            4 => self.add_or_replace_documents(index_name, random_documents).await,
            5 => self.delete_document(index_name, &random_doc_idx.to_string()).await,
            6 => self.delete_documents(index_name, random_documents.iter().map(|doc| doc.id)).await,
            _ => self.search_document(index_name, random_query, random_limit).await
        }    
    }

    pub async fn ping(&self) -> Result<String, Box<dyn Error>> {
        self.get_indexes().await
    }

    pub async fn purge(&self) -> Result<String, Box<dyn Error>> {
        let indexes : Vec<Index> = reqwest::get(self.indexes_resourse.as_str()).await?.json().await?;
        let client = reqwest::Client::new();
        //let mut log = String::new();
        let futures: Vec<_> = indexes.iter()
            .map(|index| self.indexes_resourse.join(&index.uid).unwrap())
            .map(|index_url| client.delete(index_url.as_str()).send())
            .collect();

        let responses = join_all(futures).await;
        for (response, index_name) in responses.iter().zip(indexes.iter()) {
            match response {
                Ok(response) => {
                    match response.status() {
                        StatusCode::NO_CONTENT => eprintln!("Deleted index: {}", index_name.uid).await,
                        StatusCode::NOT_FOUND => eprintln!("Cannot find index: {}", index_name.uid).await,
                        status_code => eprintln!("Unknown response {:?} from index: {}", status_code, index_name.uid).await
                    };
                },
                Err(_) => {
                    eprintln!("Failed to send request to index {}", index_name.uid).await;
                },
            }
        }
        Ok( "".to_string() )
    }

    pub async fn stress_test<'a, 'b, I>(&'a self, (mut initial_data, documents): (I, usize), queries_number: u32) -> Result<StressTestResult, Box<dyn Error>>
    where I: Iterator<Item=Document<'b>> + Clone {
        let index_name = Arc::new(format!("stresstest_{}", chrono::Local::now().format("%v_%H-%M-%S")));
        
        eprintln!(" >>> Creating index: {}", index_name).await;
        self.add_index(&index_name, "id").await?;

        eprintln!(" >>> Pushing {} documents", documents).await;
        let initial_data_clone = initial_data.clone();
        loop {
            const CHUNK_SIZE: usize = 5000; 
            let chunk = initial_data.clone().take(CHUNK_SIZE);
            self.add_or_replace_documents(&index_name, chunk).await?;
            print!("|").await;
            if initial_data.by_ref().skip(CHUNK_SIZE).next().is_none() { 
                // bug: next() retrieves an element and it's discarded, instad of pushed to server
                break;
            }
        }
        
        eprintln!("\n >>> Sending queries").await;
        let search_queries = ["indian dish", "vegetarian", "dry fruit", "fresh fish", "all-in-one", "chocolate", "sunflower oil"];
        let begin_time = Instant::now();
        let requests: Vec<_> = (0..queries_number)
            .map(|_| self.make_random_query(index_name.clone(), (1, documents), initial_data_clone.clone(), &search_queries))
            .collect();

        let requests_time = Instant::now() - begin_time;
        eprintln!(" >>> Waiting for responses").await;
        for (idx, result) in join_all(requests).await.iter().enumerate() {
            if let Err(e) = result {
                eprintln!("Query [{}] error: {}", idx + 1, e).await;
            }
        }
        let response_time = Instant::now() - begin_time;

        Ok( StressTestResult {
            all_queries_send_time_ms: requests_time.as_millis(),
            all_queries_receive_time_ms: response_time.as_millis(),
        } )
    }

    pub async fn add_index(&self, name: &str, primary_key_name: &str) -> Result<String, Box<dyn Error>> {
        let response = reqwest::Client::new()
            .post(self.indexes_resourse.as_str())
            .json(&json![{"uid": name, "primaryKey": primary_key_name}])
            .send()
            .await;
        Self::handle_response(response, "ADD_INDEX", StatusCode::CREATED)
            .await
    }

    pub async fn delete_index(&self, name: &str) -> Result<String, Box<dyn Error>> {
        let index_resourse = self.indexes_resourse.join(name).unwrap();
        let response = reqwest::Client::new()
            .delete(index_resourse)
            .send()
            .await;
        Self::handle_response(response, "DELETE_INDEX", StatusCode::NO_CONTENT)
            .await
    }

    pub async fn get_indexes(&self) -> Result<String, Box<dyn Error>> {
        let response = reqwest::get(self.indexes_resourse.as_str())
            .await;
        Self::handle_response(response, "LIST_INDEXES", StatusCode::OK)
            .await
    }

    pub async fn get_index(&self, name: &str) -> Result<String, Box<dyn Error>> {
        let index_resourse = self.indexes_resourse.join(name).unwrap();
        let response = reqwest::get(index_resourse)
            .await;
        Self::handle_response(response, "GET_INDEX", StatusCode::OK)
            .await
    }

    pub async fn search_document(&self, index: &str, query: &str, limit: usize) -> Result<String, Box<dyn Error>> {
        let index_resourse = self.indexes_resourse.join(index).unwrap();
        let response = reqwest::Client::new()
            .get(index_resourse)
            .query(&[("q", query), ("limit", &limit.to_string())])
            .send()
            .await;
        Self::handle_response(response, "SEARCH_DOC", StatusCode::OK)
            .await
    }

    pub async fn get_document(&self, index: &str, primary_key_val: &str) -> Result<String, Box<dyn Error>> {
        let doc_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap()
            .join(primary_key_val).unwrap();
        let response = reqwest::get(doc_resourse)
            .await;
        Self::handle_response(response, "GET_DOC", StatusCode::OK)
            .await
    }

    pub async fn get_documents_batch(&self, index: &str, offset: usize, limit: usize) -> Result<String, Box<dyn Error>> {
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

    pub async fn add_or_replace_documents<'a, I>(&self, index: &str, docs: I) -> Result<String, Box<dyn Error>>
    where I: IntoIterator<Item=Document<'a>>, I::IntoIter: Clone {
        let docs_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap();
        let response = reqwest::Client::new()
            .post(docs_resourse)
            .json(&SerializeIterator(docs.into_iter()))
            .send()
            .await;
        Self::handle_response(response, "ADD_OR_REPLACE_DOCS", StatusCode::ACCEPTED)
            .await
    }

    pub async fn add_or_update_documents<'a, I>(&self, index: &str, docs: I) -> Result<String, Box<dyn Error>>
    where I: IntoIterator<Item=Document<'a>>, I::IntoIter: Clone{
        let docs_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap();
        let response = reqwest::Client::new()
            .put(docs_resourse)
            .json(&SerializeIterator(docs.into_iter()))
            .send()
            .await;
        Self::handle_response(response, "ADD_OR_UPDATE_DOCS", StatusCode::ACCEPTED)
            .await
    }

    pub async fn delete_document(&self, index: &str, primary_key_val: &str) -> Result<String, Box<dyn Error>> {
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

    pub async fn delete_documents<'a, I>(&self, index: &'a str, docs_ids: I) -> Result<String, Box<dyn Error>> 
    where I: IntoIterator<Item=usize>, I::IntoIter: Clone  {
        let docs_batch_resourse = self.indexes_resourse
            .join(&format!("{}/documents/delete-batch", index)).unwrap();
        let response = reqwest::Client::new()
            .post(docs_batch_resourse)
            .json(&SerializeIterator(docs_ids.into_iter()))
            .send()
            .await;
        Self::handle_response(response, "DELETE_DOCS", StatusCode::ACCEPTED)
            .await
    }
    
    async fn handle_response(reqwest_result: Result<Response, reqwest::Error>, message_prefix: &'static str, expected_code: StatusCode) -> Result<String, Box<dyn Error>> {
        match reqwest_result {
            Err(e) => { 
                eprintln!("{} :: HTTP error: {:#?}", message_prefix, e).await;
                Err(Box::new(e))
            },
            Ok(response) => {
                let received_code = response.status();
                if received_code != expected_code {
                    let response_body = response.json().await?;
                    Err(Box::new(BadHttpStatusError { expected_code, received_code, response_body, method: message_prefix }))   
                } else {
                    Ok(response.text().await?)
                }
            },
        }
    }
}

#[derive(Debug)]
struct BadHttpStatusError {
    expected_code: StatusCode,
    received_code: StatusCode,
    response_body: serde_json::Value,
    method: &'static str,
}

impl Display for BadHttpStatusError {
    fn fmt(&self, f: &mut Formatter) -> std::fmt::Result {
        write!(f, "Query type {} responded with unexpected HTTP code: \"{}\" expected, \"{}\" received", 
            self.method, self.expected_code, self.received_code) // user-facing output
    }
}

impl Error for BadHttpStatusError {}

#[tokio::test]
async fn test_meilisearch_actions() {
    let url = Url::parse("http://localhost:7700").unwrap();
    let proxy = Proxy::new(url);
    
    assert!(proxy.ping().await.is_ok());

    assert!(proxy.purge().await.is_ok());

    assert!(proxy.add_index("test_idx", "test_id").await.is_ok());
    assert!(proxy.add_index("some_idx", "id").await.is_ok());

    assert!(proxy.get_indexes().await.is_ok());
    assert!(proxy.delete_index("test_idx").await.is_ok());

    assert!(proxy.get_index("some_idx").await.is_ok());

    let mut test_data_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_data_path.push("resourses/data.csv");
    let test_data = crate::data::load_test_data(&test_data_path);
    assert!(test_data.is_ok(), "No test data file nearby");
    let test_data = test_data.unwrap();

    let documents_to_add = (0..20)
        .map(|idx| Document { 
            id: idx + 1, 
            doc: &test_data[idx],
        });

    assert!(proxy.add_or_replace_documents("some_idx", documents_to_add).await.is_ok());

    let documents_to_update = (10..30)
        .map(|idx| Document { 
            id: idx + 1, 
            doc: &test_data[idx],
        });
    assert!(proxy.add_or_update_documents("some_idx", documents_to_update).await.is_ok());
    
    std::thread::sleep(std::time::Duration::from_millis(600)); // wait for documents to upload
    
    assert!(proxy.search_document("some_idx", "rice", 10).await.is_ok());
    assert!(proxy.get_document("some_idx", "10").await.is_ok());
    
    assert!(proxy.delete_document("some_idx", "10").await.is_ok());
    std::thread::sleep(std::time::Duration::from_millis(400)); // wait for documents to upload
    
    assert!(proxy.delete_documents("some_idx", vec![10_usize, 15, 16]).await.is_ok()); // should not find 10 
    std::thread::sleep(std::time::Duration::from_millis(400));

    assert!(proxy.get_documents_batch("some_idx", 0, 9999).await.is_ok());
}
