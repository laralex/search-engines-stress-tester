use reqwest::{Url};
use async_std::{prelude::*, eprintln, eprint, print, println};
use serde_json::json;

use std::thread;
use std::time::{Duration, Instant};
use std::path::Path;

use super::meilisearch;
use super::typesense;
use crate::data::{Document, StressTestParams};

#[derive(Debug, Clone)]
pub enum Engine {
    Meilisearch(Url, Option<String>), // url + firebase-token
    Typesense(Url, String, Option<String>), // url + api-key + firebase-token
}

pub async fn handle_ping(engine: Engine) {
    eprintln!(" >>> Pinging {:?}", engine).await;
    let timeout = Duration::from_secs(5);
    let ping_result = match engine {
        Engine::Meilisearch(url, firebase_token) => 
            meilisearch::Proxy::new(url, firebase_token).ping(timeout).await,
        Engine::Typesense(url, api_key, firebase_token) => 
            typesense::Proxy::new(url, api_key, firebase_token).ping(timeout).await, //Ok(String::from("")),
    };
    match ping_result {
        Ok(true) => println!("Success! Engine's <health> is <true>").await,
        Ok(false) => println!("Engine's <health> is <false>!").await,
        Err(e) => println!("Error occured! {:#?}", e).await,
    }
}

pub async fn handle_purge(engine: Engine) {
    eprintln!(" >>> Purging all the data from {:?}", engine).await;
    let purge_result = match engine {
        Engine::Meilisearch(url, firebase_token) => meilisearch::Proxy::new(url, firebase_token).purge().await,
        Engine::Typesense(url, api_key, firebase_token) => typesense::Proxy::new(url, api_key, firebase_token).purge().await,//typesense::purge(url, api_key).await,
    };
    match purge_result {
        Err(e) => println!(" >>> HTTP request error: {:#?}", e).await,
        Ok(_) => println!(" >>> Finished").await,
    };
}

pub async fn handle_stress_test<'a>(engine: Engine, stress_params: StressTestParams<'a>) {
    assert!(stress_params.queries_total != 0, "Given queries number is 0");
    //assert!(stress_params.bytes_total != 0, "Given bytes number is 0");
    
    eprint!(" >>> Loading test data ... ").await;
    let test_data = match crate::data::load_test_data(stress_params.data_path) {
        Ok(data) => data,
        Err(e) => {
            eprintln!("Couldn't load test data file! {:#?} \nAborting.", e).await;
            return;
        }
    };
    eprintln!("{} records from {:?}", test_data.len(), stress_params.data_path).await;
    let extended_data = test_data.iter()
        .cycle()
        .take(stress_params.initial_documents)
        .enumerate()
        .map(|(idx, doc)| Document { id: (idx + 1).to_string(), doc: &doc, dummy: idx as i32,});
    
    let queries_total = stress_params.queries_total;
    let test_result = match engine {
        Engine::Meilisearch(url, firebase_token) => meilisearch::Proxy::new(url, firebase_token)
                .stress_test(extended_data, stress_params).await,
        Engine::Typesense(url, api_key, firebase_token) => typesense::Proxy::new(url, api_key, firebase_token)
            .stress_test(extended_data, stress_params).await,
    };

    // let test_result = if let Some(index_name) = stress_params.test_existing_index.clone() {
    //     match engine {
    //         Engine::Meilisearch(url, firebase_token) => meilisearch::Proxy::new(url, firebase_token)
    //                 .stress_test_existing_index(index_name.as_str(), (extended_data, stress_params.initial_documents), stress_params.queries_total).await,
    //         Engine::Typesense(url, api_key, firebase_token) => typesense::Proxy::new(url, api_key, firebase_token)
    //             .stress_test_existing_collection(index_name.as_str(), (extended_data, stress_params.initial_documents), stress_params.queries_total).await,
    //     }
    // } else {
    //     match engine {
    //         Engine::Meilisearch(url, firebase_token) => meilisearch::Proxy::new(url, firebase_token)
    //                 .stress_test((extended_data, stress_params.initial_documents), stress_params.queries_total).await,
    //         Engine::Typesense(url, api_key, firebase_token) => typesense::Proxy::new(url, api_key, firebase_token)
    //             .stress_test((extended_data, stress_params.initial_documents), stress_params.queries_total).await,
    //     }
    // };
    
    match test_result {
        Ok(stat) => println!(" >>> Stress testing is done:\nTest took: {} ms\nIncluding sending queries: {} ms\nIncluding waiting for responses: {} ms\nIncluding waiting for updates to finish: {} ms\nAverage successful response time: {} ms\nSuccessful responses: {} out of {}", // \nAverage time to get a response: {} ms 
        stat.all_queries_send_time_ms + stat.all_queries_receive_time_ms + stat.all_updates_commited_time_ms,
        stat.all_queries_send_time_ms,
        stat.all_queries_receive_time_ms,
        stat.all_updates_commited_time_ms,
        stat.avg_success_response_time,
        stat.successful_responses,
        queries_total).await,
        Err(e) => println!(" >>> Stress test finished with error!\n{:#?}", e).await,
    }
}
