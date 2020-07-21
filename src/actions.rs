use reqwest::{Url};
use async_std::{prelude::*, eprintln, eprint, print, println};

use std::thread;
use std::time::{Duration, Instant};
use std::path::Path;

use super::meilisearch;
use super::typesense;
use crate::data::{Document};

#[derive(Debug, Clone)]
pub enum Engine {
    Meilisearch(Url),
    Typesense(Url, String), // url + api-key
}

#[derive(Debug, Clone)]
pub struct StressTestParams<'a> {
    pub queries_total: u32,
    pub initial_documents: usize,
    // pub threads_number: u16,
    pub data_path: &'a Path,
}

pub async fn handle_ping(engine: Engine) {
    eprintln!(" >>> Pinging {:?}", engine).await;
    let ping_result = match engine {
        Engine::Meilisearch(url) =>  meilisearch::Proxy::new(url).ping().await,
        Engine::Typesense(url, _) => Ok(String::from("")),
    };
    match ping_result {
        Ok(_) => println!("Success!").await,
        Err(e) => println!("HTTP request error: {:#?}", e).await,
    }
}

pub async fn handle_purge(engine: Engine) {
    eprintln!(" >>> Purging all the data from {:?}", engine).await;
    let purge_result = match engine {
        Engine::Meilisearch(url) => meilisearch::Proxy::new(url).purge().await,
        Engine::Typesense(url, api_key) => typesense::purge(url, api_key).await,
    };
    match purge_result {
        Err(e) => println!(" >>> HTTP request error: {:#?}", e).await,
        Ok(_) => println!(" >>> Purge ended:").await,
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
        .map(|(idx, doc)| Document { id: idx + 1, doc: &doc});
    
    match engine {
        Engine::Meilisearch(url) => { 
            let test_result = meilisearch::Proxy::new(url)
                .stress_test((extended_data, stress_params.initial_documents), stress_params.queries_total).await;
            match test_result {
                Ok(time) => println!(" >>> Stress testing is done:\nTest took: {} ms\nIncluding {} ms required to send all queries",
                    time.all_queries_receive_time_ms, time.all_queries_send_time_ms, ).await,
                Err(e) => println!(" >>> Stress test finished with error!\n{:#?}", e).await,
            }
        },
        Engine::Typesense(url, api_key) => (),
    };
}




// pub async fn handle_stress_test<'a>(engine: Engine, stress_params: StressTestParams<'a>) {
//     // assert!(stress_params.threads_number != 0, "Given threads number is 0");
//     assert!(stress_params.queries_total != 0, "Given queries number is 0");
//     assert!(stress_params.bytes_total != 0, "Given bytes number is 0");
    
//     // println!(" >>> Stress testing\nSearch engine: {:?}\nQueries: {}\nBytes: {}\nThreads number: {}", 
//         // engine, stress_params.queries_total, stress_params.bytes_total, stress_params.threads_number);

//     print!(" >>> Loading test data ... ");
//     let test_data = load_test_data(stress_params.data_path).expect("Couldn't load test data file!");
//     println!("{} records", test_data.len());
//     match engine {
//         Engine::Meilisearch(url) => meilisearch::Proxy::new(url).stress_test().await,
//         Engine::Typesense(url, api_key) => (),
//     };
//     return;
//     // let mut threads = Vec::with_capacity(stress_params.threads_number as usize);
//     // println!(" >>> Beginning...\nLaunching {} stress testing threads", stress_params.threads_number);
//     // let start_ts = Instant::now();
//     // for t in 0..stress_params.threads_number {
//     //     let engine_thread_local = engine.clone();
//     //     let queries_total = (stress_params.queries_total - 1).div_floor(&(stress_params.threads_number)) + 1; // divide with ceiling
//     //     let bytes_total = (stress_params.bytes_total - 1).div_floor(&(stress_params.threads_number as u32)) + 1;
//     //     threads.push(
//     //         thread::spawn(move || thread_stress_test(t, engine_thread_local, queries_total, bytes_total))
//     //     );    
//     // }
//     // let mut unfinished_threads_cnt = stress_params.threads_number;
//     // let mut threads_total_time_ms = 0;
//     // for thread in threads {
//     //     if let Ok(duration) = thread.join() {
//     //         unfinished_threads_cnt -= 1;
//     //         threads_total_time_ms += duration;
//     //     }
//     // }
//     // let test_duration = start_ts.elapsed();
//     // println!(" >>> Stress test is done!\nSuccessfully finished threads: {}\nUnsuccessfully finished threads: {}", 
//     //     stress_params.threads_number - unfinished_threads_cnt, unfinished_threads_cnt);
//     // println!("Clock time duration: {} ms\nThreads cumulative work: {} ms", test_duration.as_millis(), threads_total_time_ms);
// }

// fn thread_stress_test(thread_id: u16, engine: Engine, queries: u16, bytes_total: u32) -> u128 {
//     println!("Thread <{}>: Beginning a test with {} queries ({} bytes total)", 
//         thread_id, queries, bytes_total);
//     let start_ts = Instant::now();

//     thread::sleep(Duration::from_secs(2));
//     let working_time_ms = start_ts.elapsed().as_millis();
//     println!("Thread <{}> Finished", thread_id);
//     working_time_ms
// }