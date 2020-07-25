use futures::future::join_all;
use std::ops::Deref;
use serde::{ Serialize, Deserialize };
use serde_json::{ json };
use reqwest::{ StatusCode, Url, Response, header::{HeaderMap, HeaderValue} };
use tokio::time::{ Duration, Instant };
use async_std::{prelude::*, eprintln, eprint, print, println};
use rand::seq::{SliceRandom, IteratorRandom};
use rand::Rng;
use itertools::Itertools;
use std::sync::Arc;

use std::fmt::Write;
use std::error::Error; 

use crate::data::*;

// #[derive(Deserialize)]
// struct TypesenseCollections
// {
//     collections: Option<Vec<TypesenseCollection>>,
// }

#[derive(Deserialize, Debug)]
struct Collection 
{
    name: String,
}

pub const TYPESENSE_TEST_DOCUMENT_FIELDS: &str = "[
    {\"name\": \"title\", \"type\": \"string\" },
    {\"name\": \"price_str\", \"type\": \"string\" },
    {\"name\": \"thumbnail_path\", \"type\": \"string\", \"facet\": true },
    {\"name\": \"about\", \"type\": \"string\" },
    {\"name\": \"url\", \"type\": \"string\", \"facet\": true },
  ]";

#[derive(Serialize, Clone)]
pub struct CollectionField {
    name: String,
    r#type: String,
    facet: bool,
}

impl CollectionField {
    fn new(name: &str, field_type: &str) -> Self {
        Self {
            name: name.to_string(),
            r#type: field_type.to_string(),
            facet: false
        }
    }

    fn new_facet(name: &str, field_type: &str) -> Self {
        Self {
            name: name.to_string(),
            r#type: field_type.to_string(),
            facet: true
        }
    }
}

pub struct Proxy { 
    pub base_url: Url,
    collections_resourse: Url,
    pub api_key: String,
    common_headers: HeaderMap,
}

impl Proxy {
    pub fn new(base_url: Url, api_key: String) -> Self {
        let collections_resourse = (&base_url).join("collections/").unwrap();
        let mut common_headers = HeaderMap::new();
        common_headers.append("X-TYPESENSE-API-KEY", api_key.parse().unwrap());
        Self {
            base_url,
            collections_resourse,
            api_key,
            common_headers,
        }
    }

    async fn make_random_query<'a, I, N>(&self, collection_name: N, doc_idx_range: (usize, usize), docs: I, search_queries: &[&'a str], search_by_fields: &[&'a str]) -> Result<(serde_json::Value, Duration), Box<dyn Error>>
    where I: IntoIterator<Item=Document<'a>>, I::IntoIter: Clone, N: Deref, N::Target: AsRef<str> {
        const MAX_LIMIT: usize = 20;
        let mut rng = rand::thread_rng();
        let random_doc_idx = rng.gen_range(doc_idx_range.0, doc_idx_range.1);
        let random_limit = rng.gen_range(1, MAX_LIMIT);
        assert!(search_queries.len() > 0);
        let random_query = search_queries.choose(&mut rng).unwrap();
        let collection_name = (*collection_name).as_ref();
        let random_documents = docs.into_iter().choose_multiple(&mut rng, random_limit);
        // let random_document = docs.into_iter().choose(&mut rng).unwrap();
        let begin_time = Instant::now();
        
        let response = match rng.gen_range(0_u32, 8) {
            0 | 1 => self.get_collections().await,
            2 | 3 => self.get_document(collection_name, &random_doc_idx.to_string()).await,
            // 2 => self.get_documents_batch(collection_name,  rng.gen_range(0, (doc_idx_range.1 - doc_idx_range.0) / 2), random_limit).await,
            // 3 => self.add_or_update_documents(collection_name, random_documents).await,
            // 2 => self.import_documents(collection_name, docs.into_iter()).await,
            4 => self.import_documents(collection_name, random_documents.into_iter()).await,
            5 => self.delete_document(collection_name, &random_doc_idx.to_string()).await,
            // 6 => self.delete_documents(collection_name, random_documents.iter().map(|doc| doc.id)).await,
            6 | 7 | _ => self.search_documents(collection_name, random_query, random_limit, search_by_fields.into_iter().map(|f| *f)).await
        }; 
        match response {
            Ok(response_str) => Ok((response_str, Instant::now() - begin_time)),
            Err(e) => Err(e),
        }
    }

    pub async fn ping(&self, timeout: Duration) -> Result<bool, Box<dyn Error>> {
        let health_resourse = self.base_url.join("health").unwrap();
        let client = reqwest::Client::new();
        let response = client.get(health_resourse)
            .timeout(timeout)
            .send()
            .await;
        //Ok( true )
        let response_json: serde_json::Value = Self::handle_response(response, "HEALTH_CHECK", StatusCode::OK)
            .await?;  
        if let serde_json::Value::Bool(is_healthy) = response_json.get("ok").unwrap() {
            Ok(*is_healthy)
        } else {
            Ok(false)
        }
    }

    pub async fn purge(&self) -> Result<serde_json::Value, Box<dyn Error>> {
        let collections : Vec<Collection> = serde_json::from_value(self.get_collections().await?)?;
        let futures: Vec<_> = collections.iter()
            .map(|collection| self.delete_collection(collection.name.as_ref()))
            .collect();

        let responses = join_all(futures).await;
        for (response, collection) in responses.iter().zip(collections.iter()) {
            match response {
                Ok(response) => {
                    match response.status() {
                        StatusCode::NO_CONTENT | StatusCode::OK => eprintln!("Successfully queued collection for delete: {}", collection.name).await,
                        StatusCode::NOT_FOUND => eprintln!("Cannot find collection: {}", collection.name).await,
                        StatusCode::UNAUTHORIZED => eprintln!("Wrong API key when accessing index: {}", collection.name).await,
                        status_code => eprintln!("Unknown response {:?} from collection: {}", status_code, collection.name).await
                    };
                },
                Err(e) => {
                    eprintln!("Failed to send request to collection {} {:#?}", collection.name, e).await;
                },
            }
        }
        Ok( json!(serde_json::Value::Null) )
    }

    pub async fn stress_test<'a, 'b, I>(&'a self, (mut initial_data, documents): (I, usize), queries_number: u32) -> Result<StressTestResult, Box<dyn Error>>
    where I: Iterator<Item=Document<'b>> + Clone {
        let collection_name = Arc::new(format!("stresstest_{}", chrono::Local::now().format("%v_%H-%M-%S")));
        eprintln!(" >>> Checking if Typesense is not responding").await;
        self.ping(Duration::from_secs(5)).await?;

        eprintln!(" >>> Creating collection: {}", collection_name).await;
        let test_collection_fields = vec![
            CollectionField::new_facet("id", "string"),
            CollectionField::new("title", "string"),
            CollectionField::new("price_str", "string"),
            CollectionField::new_facet("thumbnail_path", "string"),
            CollectionField::new("about", "string"),
            CollectionField::new_facet("url", "string"),
            CollectionField::new_facet("dummy", "int32"),
        ];
        self.add_collection(&collection_name, test_collection_fields,Some("dummy")).await?;

        eprintln!(" >>> Pushing {} documents", documents).await;
        let initial_data_clone = initial_data.clone();
        let mut update_ids = vec![];
        loop {
            const CHUNK_SIZE: usize = 1500; 
            let chunk = initial_data.clone().take(CHUNK_SIZE);
            let update_json = self.import_documents(&collection_name, chunk).await?;
            if let Some(serde_json::Value::Number(update_id)) = update_json.get("updateId") {
                update_ids.push(update_id.as_u64().unwrap());
            }
            eprint!(".").await;
            if initial_data.by_ref().skip(CHUNK_SIZE).next().is_none() { 
                // bug: next() retrieves an element and it's discarded, instad of pushed to server
                break;
            }
        }

        eprintln!("\n >>> Sending {} queries", queries_number).await;
        let search_queries = ["indian dish", "vegetarian", "dry fruit", "fresh fish", "all-in-one", "chocolate", "sunflower oil"];
        let search_by_fields = vec!["title", "about", "price_str", "url", "thumbnail_path", "id"];
        let begin_time = Instant::now();
        let requests: Vec<_> = (0..queries_number)
            .map(|_| self.make_random_query(collection_name.clone(), (1, documents), initial_data_clone.clone(), &search_queries, &search_by_fields))
            .collect();

        let requests_time = Instant::now() - begin_time;
        eprintln!(" >>> Waiting for {} responses (might be long operation)", queries_number).await;
        
        let mut successful_responses = 0_u32;
        let mut avg_duration = Duration::from_millis(0);
        let begin_time = Instant::now();
        for (idx, result) in join_all(requests).await.iter().enumerate() {
            match result {
                Ok((_, duration)) => {
                    successful_responses += 1;
                    avg_duration += *duration;
                },
                Err(e) => eprintln!("Query [{}] error: {}", idx + 1, e).await,
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

        Ok( StressTestResult {
            all_queries_send_time_ms: requests_time.as_millis(),
            all_queries_receive_time_ms: response_time.as_millis(),
            all_updates_commited_time_ms: 0,
            avg_response_time: (avg_duration / successful_responses).as_millis(),
        } )
    }

    pub async fn add_collection(&self, collection_name: &str, fields: Vec<CollectionField>, sorting_field: Option<&str>) -> Result<serde_json::Value, Box<dyn Error>> {
        let client = reqwest::Client::new();
        let response = client.post(self.collections_resourse.as_str())
            .headers(self.common_headers.clone())
            .json(&json!({ 
                "name": collection_name, 
                "fields": fields,
                "default_sorting_field": sorting_field, 
            }))
            .send()
            .await;
        Self::handle_response(response, "ADD_COLLECTION", StatusCode::CREATED)
            .await
    }

    pub async fn get_collections(&self) -> Result<serde_json::Value, Box<dyn Error>> {
        let client = reqwest::Client::new();
        //eprintln!("get_col {}", self.collections_resourse).await;
        let response = client.get(self.collections_resourse.as_str())
            .headers(self.common_headers.clone())
            .send()
            .await;
        Self::handle_response(response, "LIST_COLLECTIONS", StatusCode::OK)
            .await
    }

    pub async fn delete_collection(&self, name: &str) -> Result<Response, Box<dyn Error>> {
        let collection_resourse = self.collections_resourse.join(name).unwrap();
        let response = reqwest::Client::new()
            .delete(collection_resourse)
            .headers(self.common_headers.clone())
            .send()
            .await;
        Self::handle_response_forward(response, "DELETE_COLLECTION", StatusCode::OK)
            .await
    }

    pub async fn get_document(&self, collection: &str, primary_key_val: &str) -> Result<serde_json::Value, Box<dyn Error>> {
        let doc_resourse = self.collections_resourse
            .join(&format!("{}/documents/", collection)).unwrap()
            .join(primary_key_val).unwrap();
        let client = reqwest::Client::new();
        let response = client.get(doc_resourse)
            .headers(self.common_headers.clone())
            .send()
            .await;
        Self::handle_response(response, "GET_DOC", StatusCode::OK)
            .await
    }

    pub async fn add_document<'a>(&self, collection: &str, doc: Document<'a>) -> Result<serde_json::Value, Box<dyn Error>> {
        let collection_resourse = self.collections_resourse
            .join(&format!("{}/documents/", collection)).unwrap();
        let response = reqwest::Client::new().post(collection_resourse)
            .headers(self.common_headers.clone())
            .json(&doc)
            .send()
            .await;
        Self::handle_response(response, "ADD_DOC", StatusCode::CREATED)
            .await
    }

    pub async fn import_documents<'a>(&self, collection: &str, docs: impl Iterator<Item=Document<'a>> + Clone) -> Result<serde_json::Value, Box<dyn Error>> {
        let collection_resourse = self.collections_resourse
            .join(&format!("{}/documents/import", collection)).unwrap();
        let response = reqwest::Client::new().post(collection_resourse)
            .headers(self.common_headers.clone())
            .body(format!("{}", TypesenseDocuments(docs))) // new line separated
            .send()
            .await;
        Self::handle_response(response, "IMPORT_DOCS", StatusCode::OK)
            .await
    }

    pub async fn search_documents<'a, I>(&self, collection: &str, query: &str, limit: usize, mut query_by: I) -> Result<serde_json::Value, Box<dyn Error>>
    where I: Iterator<Item=&'a str> {
        let collection_resourse = self.collections_resourse
            .join(&format!("{}/documents/search", collection)).unwrap();
        // eprintln!("{:#?}", reqwest::Client::new()
        //     .get(collection_resourse.clone())
        //     .headers(self.common_headers.clone())
        //     .query(&[("q", query), ("max_hits", &limit.to_string()), ("query_by", &query_by.join(","))])).await;
        let response = reqwest::Client::new()
            .get(collection_resourse)
            .headers(self.common_headers.clone())
            // .query(&[("q", query), ("query_by", query_by.next().unwrap()), ("max_hits", &limit.to_string()), ])
            .query(&[("q", query), ("query_by", &query_by.join(",")), ("max_hits", &limit.to_string()), ])
            .send()
            .await;
        //eprintln!("{:#?}", response.json().await).await;
        Self::handle_response(response, "SEARCH_DOCS", StatusCode::OK)
            .await
    }

    pub async fn delete_document(&self, collection: &str, primary_key_val: &str) -> Result<serde_json::Value, Box<dyn Error>> {
        let doc_resourse = self.collections_resourse
            .join(&format!("{}/documents/{}", collection, primary_key_val)).unwrap();
        let response = reqwest::Client::new()
            .delete(doc_resourse)
            .headers(self.common_headers.clone())
            .send()
            .await;
        Self::handle_response(response, "DELETE_DOC", StatusCode::OK)
            .await
    }

    async fn handle_response_forward(reqwest_result: Result<Response, reqwest::Error>, message_prefix: &'static str, expected_code: StatusCode) -> Result<Response, Box<dyn Error>> {
        match reqwest_result {
            Err(e) => { 
                eprintln!("{} :: HTTP error: {:#?}", message_prefix, e).await;
                Err(Box::new(e))
            },
            Ok(response) => {
                let received_code = response.status();
                if received_code != expected_code {
                    let response_body = response.json().await?;
                    // eprintln!("{:#?}", response_body).await;
                    Err(Box::new(BadHttpStatusError { expected_code, received_code, response_body, method: message_prefix }))   
                } else {
                    Ok(response)
                }
            },
        }
    }

    async fn handle_response(reqwest_result: Result<Response, reqwest::Error>, message_prefix: &'static str, expected_code: StatusCode) -> Result<serde_json::Value, Box<dyn Error>> {
        match Self::handle_response_forward(reqwest_result, message_prefix, expected_code).await {
            // Ok(response) => { let json = response.json().await?; eprintln!("{:#?}", json); Ok(json) },
            Ok(response) => Ok(response.json().await?),
            Err(e) => Err(e),
        }
    }
}

#[tokio::test]
async fn test_typesense_actions() {
    let url = Url::parse("http://localhost:8108").unwrap();
    let proxy = Proxy::new(url, "ABCD".to_string());
    
    assert!(proxy.ping(Duration::from_secs(3)).await.is_ok());

    assert!(proxy.purge().await.is_ok());

    std::thread::sleep(std::time::Duration::from_millis(1000));

    let test_collection_fields = vec![
        CollectionField::new_facet("id", "string"),
        CollectionField::new("title", "string"),
        CollectionField::new("price_str", "string"),
        CollectionField::new_facet("thumbnail_path", "string"),
        CollectionField::new("about", "string"),
        CollectionField::new_facet("url", "string"),
        CollectionField::new_facet("dummy", "int32")
    ];
    // eprintln!("{:#?}", proxy.add_collection("test_idx2", test_collection_fields.clone(), Some("id")).await).await;
    let ttest = proxy.add_collection("ttest_idx", test_collection_fields.clone(), Some("dummy")).await;
    // eprintln!("{:#?}", ttest).await;
    assert!(ttest.is_ok());

    let tsome = proxy.add_collection("tsome_idx", test_collection_fields.clone(), Some("dummy")).await;
    // eprintln!("{:#?}", tsome).await;
    assert!(tsome.is_ok());

    //eprintln!("{:#?}", proxy.get_collections().await).await;
    assert!(proxy.get_collections().await.is_ok());
    assert!(proxy.delete_collection("ttest_idx").await.is_ok());

    // assert!(proxy.get_collection("tsome_idx").await.is_ok());

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

    // eprintln!("{:#?}", proxy.import_documents("tsome_idx", documents_to_add).await).await;
    assert!(proxy.import_documents("tsome_idx", documents_to_add).await.is_ok());

    // std::thread::sleep(std::time::Duration::from_millis(600)); // wait for documents to upload
    let search_by_fields = vec!["title", "about", "price_str", "url", "thumbnail_path", "id"];
    let search = proxy.search_documents("tsome_idx", "rice", 10, search_by_fields.into_iter()).await;
    // eprintln!("{:#?}", search).await;
    assert!(search.is_ok());

    let get = proxy.get_document("tsome_idx", "10").await;
    // eprintln!("{:#?}", get).await;
    assert!(get.is_ok());
    
    assert!(proxy.delete_document("tsome_idx", "10").await.is_ok());
}