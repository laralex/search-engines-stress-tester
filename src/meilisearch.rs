
use serde::Deserialize;
use serde_json::{json};
use reqwest::{StatusCode, Url, Response, header::HeaderMap};
use rand::seq::{SliceRandom, IteratorRandom};
use rand::Rng;
use futures::prelude::*;
use futures::future::{join_all};
use tokio::time::{ Duration, Instant };
use itertools::Itertools;
use async_std::{prelude::*, eprintln, eprint, print, println};

use std::{iter::IntoIterator, fmt::Write};
use std::sync::Arc;
use std::error::Error;
use std::fmt::{Display, Formatter};
use std::ops::Deref;
use std::collections::LinkedList;

use crate::data::*;

#[derive(Deserialize)]
struct Index
{
    pub uid: String,
}
#[derive(Deserialize)]
struct Update
{
    pub status: String,
}

pub struct Proxy { 
    pub base_url: Url,
    // pub firebase_token: Option<String>,
    indexes_resourse: Url,
    common_headers: HeaderMap,
}

impl Proxy {
    pub fn new(base_url: Url, firebase_token: Option<String>) -> Self {
        let indexes_resourse = (&base_url).join("indexes/").unwrap();
        let mut common_headers = HeaderMap::new();
        if let Some(token) = firebase_token {
            //eprintln!("Firebase token in use: {}", token);
            common_headers.append("x-firebase-token", token.parse().unwrap());
        }
        Self {
            base_url,
            // firebase_token,
            indexes_resourse,
            common_headers,
        }
    }

    async fn make_random_query<'a, I, N>(&self, index_name: N, doc_idx_range: (usize, usize), docs: I, search_queries: &[&'a str]) -> Result<(serde_json::Value, Duration), Box<dyn Error>>
    where I: IntoIterator<Item=Document<'a>>, I::IntoIter: Clone, N: Deref, N::Target: AsRef<str> {
        const MAX_LIMIT: usize = 20;
        let mut rng = rand::thread_rng();
        let random_doc_idx = rng.gen_range(doc_idx_range.0, doc_idx_range.1);
        let random_limit = rng.gen_range(1, MAX_LIMIT);
        assert!(search_queries.len() > 0);
        let random_query = search_queries.choose(&mut rng).unwrap();
        let index_name = (*index_name).as_ref();
        let random_documents = docs.into_iter().choose_multiple(&mut rng, random_limit);
        //let random_document = docs.into_iter().choose(&mut rng).unwrap();
        let begin_time = Instant::now();
        let response = match rng.gen_range(0_u32, 8) {
            0 | 1 => self.get_indexes().await,
            2 | 3 => self.get_document(index_name, &random_doc_idx.to_string()).await,
            //2 => self.get_documents_batch(index_name,  rng.gen_range(0, (doc_idx_range.1 - doc_idx_range.0) / 2), random_limit).await,
            //3 => self.add_or_update_documents(index_name, random_documents).await,
            4 => self.add_or_replace_documents(index_name, random_documents).await,
            5 => self.delete_document(index_name, &random_doc_idx.to_string()).await,
            //6 => self.delete_documents(index_name, random_documents.iter().map(|doc| doc.id)).await,
            6 | 7 | _ => self.search_documents(index_name, random_query, random_limit).await
        }; 
        match response {
            Ok(response_str) => Ok((response_str, Instant::now() - begin_time)),
            Err(e) => Err(e),
        }
    }

    pub async fn ping(&self, timeout: Duration) -> Result<bool, Box<dyn Error>> {
        let health_resourse = self.base_url.join("health").unwrap();
        let client = reqwest::Client::new();
        let _response = client.get(health_resourse)
            .timeout(timeout)
            .send()
            .await?;
        Ok( true )
        // Self::handle_response(response, "HEALTH_CHECK", StatusCode::NO_CONTENT)
        //     .await?  
        // if let serde_json::Value::Bool(health) = response_json.get("/health").unwrap() {
        //     Ok(*health)
        // } else {
        //     Ok(false)
        // }
    }

    pub async fn purge(&self) -> Result<serde_json::Value, Box<dyn Error>> {
        let client = reqwest::Client::new();

        let indexes : Vec<Index> = client.get(self.indexes_resourse.as_str())
            .headers(self.common_headers.clone())
            .send()
            .await?
            .json()
            .await?;

        //let mut log = String::new();
        let futures: Vec<_> = indexes.iter()
            .map(|index| self.indexes_resourse.join(&index.uid).unwrap())
            .map(|index_url| client.delete(index_url.as_str())
                .headers(self.common_headers.clone())
                .send())
            .collect();

        let responses = join_all(futures).await;
        for (response, index_name) in responses.iter().zip(indexes.iter()) {
            match response {
                Ok(response) => {
                    match response.status() {
                        StatusCode::NO_CONTENT => eprintln!("Successfully queued index for delete: {}", index_name.uid).await,
                        StatusCode::NOT_FOUND => eprintln!("Cannot find index: {}", index_name.uid).await,
                        StatusCode::UNAUTHORIZED => eprintln!("Wrong API key when accessing index: {}", index_name.uid).await,
                        status_code => eprintln!("Unknown response {:?} from index: {}", status_code, index_name.uid).await
                    };
                },
                Err(_) => {
                    eprintln!("Failed to send request to index {}", index_name.uid).await;
                },
            }
        }
        Ok( json!(serde_json::Value::Null) )
    }

    pub async fn stress_test<'a, 'b, I>(&'a self, mut initial_data: I, test_params: StressTestParams<'a>) -> Result<StressTestResult, Box<dyn Error>>
    where I: Iterator<Item=Document<'b>> + Clone {
        
        eprintln!(" >>> Checking if Meilisearch is not responding").await;
        self.ping(Duration::from_secs(5)).await?;

        let initial_data_clone = initial_data.clone();

        let index_name;
        if let Some(index_name_ref) = test_params.test_existing_index.as_ref() {
            index_name = Arc::new(index_name_ref.to_owned());
        } else {
            index_name = Arc::new(format!("stresstest_{}", chrono::Local::now().format("%v_%H-%M-%S")));

            eprintln!(" >>> Creating index: {}", index_name).await;
            self.add_index(&index_name, "id").await?;

            eprintln!(" >>> Pushing {} documents", test_params.initial_documents).await;
            let mut update_ids = vec![];
            loop {
                const CHUNK_SIZE: usize = 1500; 
                let chunk = initial_data.clone().take(CHUNK_SIZE);
                let update_json = self.add_or_replace_documents(&index_name, chunk).await?;
                if let Some(serde_json::Value::Number(update_id)) = update_json.get("updateId") {
                    update_ids.push(update_id.as_u64().unwrap());
                }
                eprint!(".").await;
                if initial_data.by_ref().skip(CHUNK_SIZE).next().is_none() { 
                    // bug: next() retrieves an element and it's discarded, instad of pushed to server
                    break;
                }
            }
            eprintln!("\n >>> Waiting for documents to be stored (long operation, ~10 secs per 10 000 documents)").await;
            let mut ping_errors_limit = 100;
            for update_id in update_ids {
                loop {
                    match self.is_update_finished(&index_name, update_id).await {
                        Ok(Some(true)) | Ok(None) => break,
                        Ok(Some(false)) => async_std::task::sleep(std::time::Duration::from_millis(1000)).await,
                        Err(e) => {
                            //ping_errors_limit -= 1;
                            if ping_errors_limit == 0 { return Err(e); }
                        },
                    }
                }
                eprint!(".").await;
            }
        }
        
        eprintln!("\n >>> Sending {} queries", test_params.queries_total).await;
        let search_queries = ["indian dish", "vegetarian", "dry fruit", "fresh fish", "all-in-one", "chocolate", "sunflower oil"];
        let begin_time = Instant::now();
        let requests: Vec<_> = (0..test_params.queries_total)
            .map(|_| self.make_random_query(index_name.clone(), (1, test_params.initial_documents), initial_data_clone.clone(), &search_queries))
            .collect();

        let requests_time = Instant::now() - begin_time;
        eprintln!(" >>> Waiting for {} responses (long operation, ~1 sec per 10 queries)", test_params.queries_total).await;
        
        let mut successful_responses = 0_u32;
        let mut avg_success_duration = Duration::from_millis(0);
        let begin_time = Instant::now();
        for (idx, result) in join_all(requests).await.iter().enumerate() {
            match result {
                Ok((_, duration)) => {
                    successful_responses += 1;
                    avg_success_duration += *duration;
                },
                Err(e) => eprintln!("Query [{}] error: {}", idx + 1, e.to_string()).await,
            }
        }

        // Sequential wait (slower, but prettier output)
        // let mut requests_cnt = 0_usize;
        // for request in requests {
        //     requests_cnt += 1;
        //     eprintln!("\rQuery [{}/{}]", requests_cnt, queries).await;
        //     if let Err(e) = request.await {
        //         eprintln!("Query [{}] error: {}", requests_cnt, e).await;
        //     }
        // }

        let response_time = Instant::now() - begin_time;

        // wait for all updates
        let begin_time = Instant::now();
        if !test_params.no_wait_after_updates {
            eprintln!(" >>> Waiting for all async updates to finish").await;
            loop {
                let updates: Vec<Update> = serde_json::from_value(self.get_all_updates(&index_name).await?)?;
                let unprocessed_cnt = updates.iter().filter(|s| s.status != "processed").count();
                eprintln!("Unfinished updates: {}", unprocessed_cnt).await;
                // eprint!(".").await;
                if unprocessed_cnt == 0 { break; }
                std::thread::sleep(Duration::from_millis(std::cmp::max(50*unprocessed_cnt as u64, 2000)));
            }
        }
        
        let update_time = Instant::now() - begin_time;
        Ok( StressTestResult {
            all_queries_send_time_ms: requests_time.as_millis(),
            all_queries_receive_time_ms: response_time.as_millis(),
            all_updates_commited_time_ms: update_time.as_millis(),
            avg_success_response_time: (avg_success_duration / successful_responses).as_millis(),
            successful_responses,
        } )
    }

    pub async fn is_update_finished(&self, index_name: &str, update_id: u64) -> Result<Option<bool>, Box<dyn Error>> {
        let update_resourse = self.indexes_resourse
            .join(&format!("{}/updates/", index_name)).unwrap()
            .join(&update_id.to_string()).unwrap();
        let response = reqwest::Client::new()
            .get(update_resourse)
            .headers(self.common_headers.clone())
            .send()
            .await;
        let response_json = Self::handle_response(response, "IS_UPDATE_FINISHED", StatusCode::OK)
            .await?;
        match response_json.get("status") {
            Some(serde_json::Value::String(value)) => Ok(Some(value == "processed")),
            _ => Ok(None),
        }
    }

    pub async fn add_index(&self, name: &str, primary_key_name: &str) -> Result<serde_json::Value, Box<dyn Error>> {
        let response = reqwest::Client::new()
            .post(self.indexes_resourse.as_str())
            .headers(self.common_headers.clone())
            .json(&json![{"uid": name, "primaryKey": primary_key_name}])
            .send()
            .await;
        Self::handle_response(response, "ADD_INDEX", StatusCode::CREATED)
            .await
    }

    pub async fn delete_index(&self, name: &str) -> Result<(), Box<dyn Error>> {
        let index_resourse = self.indexes_resourse.join(name).unwrap();
        let response = reqwest::Client::new()
            .delete(index_resourse)
            .headers(self.common_headers.clone())
            .send()
            .await;
        Self::handle_response_forward(response, "DELETE_INDEX", StatusCode::NO_CONTENT)
            .await?;
        Ok(())
    }

    pub async fn get_indexes(&self) -> Result<serde_json::Value, Box<dyn Error>> {
        let response = reqwest::Client::new()
            .get(self.indexes_resourse.as_str())
            .headers(self.common_headers.clone())
            .send()
            .await;
        // if let Some(timeout) = timeout {
        //     let response = request_fut.timeout(timeout).await?;
        //     return Self::handle_response(response, "LIST_INDEXES", StatusCode::OK)
        //         .await
        // }
        Self::handle_response(response, "LIST_INDEXES", StatusCode::OK)
                .await
    }

    pub async fn get_index(&self, name: &str) -> Result<serde_json::Value, Box<dyn Error>> {
        let index_resourse = self.indexes_resourse.join(name).unwrap();
        let response = reqwest::Client::new()
            .get(index_resourse)
            .headers(self.common_headers.clone())
            .send()
            .await;
        Self::handle_response(response, "GET_INDEX", StatusCode::OK)
            .await
    }

    pub async fn search_documents(&self, index: &str, query: &str, limit: usize) -> Result<serde_json::Value, Box<dyn Error>> {
        let index_resourse = self.indexes_resourse
            .join(&format!("{}/search", index)).unwrap();
        let response = reqwest::Client::new()
            .get(index_resourse)
            .headers(self.common_headers.clone())
            .query(&[("q", query), ("limit", &limit.to_string())])
            .send()
            .await;
        Self::handle_response(response, "SEARCH_DOC", StatusCode::OK)
            .await
    }

    pub async fn get_document(&self, index: &str, primary_key_val: &str) -> Result<serde_json::Value, Box<dyn Error>> {
        let doc_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap()
            .join(primary_key_val).unwrap();
        let response = reqwest::Client::new()
            .get(doc_resourse)
            .headers(self.common_headers.clone())
            .send()
            .await;
        Self::handle_response(response, "GET_DOC", StatusCode::OK)
            .await
    }

    pub async fn get_documents_batch(&self, index: &str, offset: usize, limit: usize) -> Result<serde_json::Value, Box<dyn Error>> {
        let docs_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap();
        let response = reqwest::Client::new()
            .get(docs_resourse)
            .headers(self.common_headers.clone())
            .query(&[("offset", &offset.to_string()), ("limit", &limit.to_string())])
            .send()
            .await;
        Self::handle_response(response, "GET_DOCS_BATCH", StatusCode::OK)
            .await
    }

    pub async fn add_or_replace_documents<'a, I>(&self, index: &str, docs: I) -> Result<serde_json::Value, Box<dyn Error>>
    where I: IntoIterator<Item=Document<'a>>, I::IntoIter: Clone {
        let docs_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap();
        let response = reqwest::Client::new()
            .post(docs_resourse)
            .headers(self.common_headers.clone())
            .json(&SerializeIterator(docs.into_iter()))
            .send()
            .await;
        Self::handle_response(response, "ADD_OR_REPLACE_DOCS", StatusCode::ACCEPTED)
            .await
    }

    pub async fn add_or_update_documents<'a, I>(&self, index: &str, docs: I) -> Result<serde_json::Value, Box<dyn Error>>
    where I: IntoIterator<Item=Document<'a>>, I::IntoIter: Clone{
        let docs_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap();
        let response = reqwest::Client::new()
            .put(docs_resourse)
            .headers(self.common_headers.clone())
            .json(&SerializeIterator(docs.into_iter()))
            .send()
            .await;
        Self::handle_response(response, "ADD_OR_UPDATE_DOCS", StatusCode::ACCEPTED)
            .await
    }

    pub async fn delete_document(&self, index: &str, primary_key_val: &str) -> Result<serde_json::Value, Box<dyn Error>> {
        let doc_resourse = self.indexes_resourse
            .join(&format!("{}/documents/", index)).unwrap()
            .join(primary_key_val).unwrap();
        let response = reqwest::Client::new()
            .delete(doc_resourse)
            .headers(self.common_headers.clone())
            .send()
            .await;
        Self::handle_response(response, "DELETE_DOC", StatusCode::ACCEPTED)
            .await
    }

    pub async fn delete_documents<'a, I>(&self, index: &'a str, docs_ids: I) -> Result<serde_json::Value, Box<dyn Error>> 
    where I: IntoIterator<Item=usize>, I::IntoIter: Clone  {
        let docs_batch_resourse = self.indexes_resourse
            .join(&format!("{}/documents/delete-batch", index)).unwrap();
        let response = reqwest::Client::new()
            .post(docs_batch_resourse)
            .headers(self.common_headers.clone())
            .json(&SerializeIterator(docs_ids.into_iter()))
            .send()
            .await;
        Self::handle_response(response, "DELETE_DOCS", StatusCode::ACCEPTED)
            .await
    }

    pub async fn get_all_updates(&self, index: &str) -> Result<serde_json::Value, Box<dyn Error>> {
        let updates_resourse = self.indexes_resourse.join(&format!("{}/updates", index)).unwrap();
        let response = reqwest::Client::new()
            .get(updates_resourse)
            .headers(self.common_headers.clone())
            .send()
            .await;
        Self::handle_response(response, "WAIT_ALL_UPDATES", StatusCode::OK)
            .await
    }

    async fn handle_response_forward(reqwest_result: Result<Response, reqwest::Error>, message_prefix: &'static str, expected_code: StatusCode) -> Result<Response, Box<dyn Error>> {
        match reqwest_result {
            Err(e) => { 
                //eprintln!("{} :: HTTP error: {:#?}", message_prefix, e).await;
                Err(Box::new(e))
            },
            Ok(response) => {
                let received_code = response.status();
                if received_code != expected_code {
                    let response_body = response.json().await?;
                    Err(Box::new(BadHttpStatusError { expected_code, received_code, response_body, method: message_prefix }))   
                } else {
                    Ok(response)
                }
            },
        }
    }

    async fn handle_response(reqwest_result: Result<Response, reqwest::Error>, message_prefix: &'static str, expected_code: StatusCode) -> Result<serde_json::Value, Box<dyn Error>> {
        match Self::handle_response_forward(reqwest_result, message_prefix, expected_code).await {
            Ok(response) => Ok(response.json().await?),
            Err(e) => Err(e),
        }
    }
}

#[tokio::test]
async fn test_meilisearch_actions() {
    let url = Url::parse("http://localhost:7700").unwrap();
    let proxy = Proxy::new(url, Some("eyJhbGciOiJSUzI1NiIsImtpZCI6IjU1NGE3NTQ3Nzg1ODdjOTRjMTY3M2U4ZWEyNDQ2MTZjMGMwNDNjYmMiLCJ0eXAiOiJKV1QifQ.eyJuYW1lIjoic2VydmljZXJlZ2lzdHJhdGlvMSIsInBpY3R1cmUiOiJodHRwOi8vd3d3LmV4YW1wbGUuY29tLzEyMzQ1Njc4L3Bob3RvLnBuZyIsImlzcyI6Imh0dHBzOi8vc2VjdXJldG9rZW4uZ29vZ2xlLmNvbS9icmluZGF2YW4tYzYxYjciLCJhdWQiOiJicmluZGF2YW4tYzYxYjciLCJhdXRoX3RpbWUiOjE1OTU5NDY1NDMsInVzZXJfaWQiOiJlY09ocjVyc2dWT1J5TkJCODRKTzNyd1VBZWwyIiwic3ViIjoiZWNPaHI1cnNnVk9SeU5CQjg0Sk8zcndVQWVsMiIsImlhdCI6MTU5NTk0NjU0MywiZXhwIjoxNTk1OTUwMTQzLCJlbWFpbCI6InNlcnZpY2VyZWdpc3RyYXRpbzFAYWJjLmNvbSIsImVtYWlsX3ZlcmlmaWVkIjpmYWxzZSwiZmlyZWJhc2UiOnsiaWRlbnRpdGllcyI6eyJlbWFpbCI6WyJzZXJ2aWNlcmVnaXN0cmF0aW8xQGFiYy5jb20iXX0sInNpZ25faW5fcHJvdmlkZXIiOiJwYXNzd29yZCJ9fQ.mog-lpAps8Efw3MXOvCpEWU_2LZ_1CfSu9xypMCH6EZ0bd-JxaodALqil2dIvbNjbPaEt_oTojvniHKD1WdBy4ISBJ9y9k5Nk-7AxozIhqnuNZU6R7d70zSP5leVRTPcPXg7LKRvw2BD__nZBmHszCsYJIznNDFnGPJI3RgjrRv4eUCwM6et0QsV8_c5sd-4DRNDi9Kdgzbrcdpaj_jFYDyovfbLUAAsfK_Oi9GhyyqUizmnsslwLSn8x1-qMT439QU_pNAg59BJp012xlFuRrppkJx35W8QCdrgoZpoRDZEj6ZgG3v3tLInu7pAK6J4hNtp6j5WuPJzXFj60LHBbA".into()));
    
    assert!(proxy.ping(Duration::from_secs(3)).await.is_ok());

    assert!(proxy.purge().await.is_ok());

    assert!(proxy.add_index("test_idx", "test_id").await.is_ok());
    assert!(proxy.add_index("some_idx", "id").await.is_ok());

    assert!(proxy.get_indexes().await.is_ok());
    // let response = proxy.delete_index("test_idx").await;
    // eprintln!("{:?}", response).await;
    assert!(proxy.delete_index("test_idx").await.is_ok());

    assert!(proxy.get_index("some_idx").await.is_ok());

    let mut test_data_path = std::path::PathBuf::from(env!("CARGO_MANIFEST_DIR"));
    test_data_path.push("resourses/data.csv");
    let test_data = crate::data::load_test_data(&test_data_path);
    assert!(test_data.is_ok(), "No test data file nearby");
    let test_data = test_data.unwrap();

    let documents_to_add = (0..20)
        .map(|idx| Document { 
            id: (idx + 1).to_string(), 
            doc: &test_data[idx],
            dummy: idx as i32,
        });

    assert!(proxy.add_or_replace_documents("some_idx", documents_to_add).await.is_ok());

    let documents_to_update = (10..30)
        .map(|idx| Document { 
            id: (idx + 1).to_string(), 
            doc: &test_data[idx],
            dummy: idx as i32,
        });
    assert!(proxy.add_or_update_documents("some_idx", documents_to_update).await.is_ok());
    
    std::thread::sleep(std::time::Duration::from_millis(600)); // wait for documents to upload
    
    assert!(proxy.search_documents("some_idx", "rice", 10).await.is_ok());
    assert!(proxy.get_document("some_idx", "10").await.is_ok());
    
    assert!(proxy.delete_document("some_idx", "10").await.is_ok());
    std::thread::sleep(std::time::Duration::from_millis(400)); // wait for documents to upload
    
    assert!(proxy.delete_documents("some_idx", vec![10_usize, 15, 16]).await.is_ok()); // should not find 10 
    std::thread::sleep(std::time::Duration::from_millis(400));

    assert!(proxy.get_documents_batch("some_idx", 0, 9999).await.is_ok());
}
