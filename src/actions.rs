use num::Integer;
use reqwest::{Error, Url};
use rand::{distributions::{Distribution, Standard}, Rng};
use csv::ReaderBuilder;
use serde::Deserialize;

use std::thread;
use std::time::{Duration, Instant};
use std::path::Path;

use super::meilisearch;
use super::typesense;

#[derive(Debug, Clone)]
pub enum Engine {
    Meilisearch(Url),
    Typesense(Url, String), // url + api-key
}

#[derive(Debug, Clone)]
pub struct StressTestParams<'a> {
    pub queries_total: u16,
    pub bytes_total: u32,
    pub threads_number: u16,
    pub data_path: &'a Path,
}

#[derive(Deserialize)]
pub struct TestDocument {
    id: usize,
    title: String,
    price_str: String,
    thumbnail_path: String,
    about: String,
    url: String,
}


pub fn handle_ping(engine: Engine) {
    println!(">>> Pinging {:?}", engine);
    match ping(engine) {
        Ok(response) => println!("Success!"),
        Err(e) => println!("HTTP request failed: {:?}", e),
    };
}

fn ping(engine: Engine) -> Result<String, Error> {
    let ping_dest = match engine {
        Engine::Meilisearch(url) => { 
            url.join("/indexes").unwrap()
        },
        Engine::Typesense(url, _) => {
            url.join("/health").unwrap()
        },
        _ => panic!("Unsupported engine"),
    };
    let response = reqwest::blocking::get(ping_dest)?.text()?;
    Ok(response)
}

pub fn handle_purge(engine: Engine) {
    println!(">>> Purging all the data from {:?}", engine);
    match purge(engine) {
        Err(e) => println!("HTTP request failed: {:?}", e),
        Ok(_) => println!("Task finished"),
    };
}

fn purge(engine: Engine) -> Result<(), Error> {
    match engine {
        Engine::Meilisearch(url) => meilisearch::purge(url)?,
        Engine::Typesense(url, api_key) => typesense::purge(url, api_key)?,
        _ => panic!("Unsupported engine"),
    }
    Ok ( () )
}

pub fn handle_stress_test(engine: Engine, stress_params: StressTestParams) {
    assert!(stress_params.threads_number != 0, "Given threads number is 0");
    assert!(stress_params.queries_total != 0, "Given queries number is 0");
    assert!(stress_params.bytes_total != 0, "Given bytes number is 0");
    
    println!(" >>> Stress testing\nSearch engine: {:?}\nQueries: {}\nBytes: {}\nThreads number: {}", 
        engine, stress_params.queries_total, stress_params.bytes_total, stress_params.threads_number);

    println!(" >>> Loading test data");
    let test_data = load_test_data(stress_params.data_path).expect("Couldn't load test data file!");
    let mut threads = Vec::with_capacity(stress_params.threads_number as usize);
    println!(" >>> Beginning...\nLaunching {} stress testing threads", stress_params.threads_number);
    let start_ts = Instant::now();
    for t in 0..stress_params.threads_number {
        let engine_thread_local = engine.clone();
        let queries_total = (stress_params.queries_total - 1).div_floor(&(stress_params.threads_number)) + 1; // divide with ceiling
        let bytes_total = (stress_params.bytes_total - 1).div_floor(&(stress_params.threads_number as u32)) + 1;
        threads.push(
            thread::spawn(move || thread_stress_test(t, engine_thread_local, queries_total, bytes_total))
        );    
    }
    let mut unfinished_threads_cnt = stress_params.threads_number;
    let mut threads_total_time_ms = 0;
    for thread in threads {
        if let Ok(duration) = thread.join() {
            unfinished_threads_cnt -= 1;
            threads_total_time_ms += duration;
        }
    }
    let test_duration = start_ts.elapsed();
    println!(" >>> Stress test is done!\nSuccessfully finished threads: {}\nUnsuccessfully finished threads: {}", 
        stress_params.threads_number - unfinished_threads_cnt, unfinished_threads_cnt);
    println!("Clock time duration: {} ms\nThreads cumulative work: {} ms", test_duration.as_millis(), threads_total_time_ms);
}

fn thread_stress_test(thread_id: u16, engine: Engine, queries: u16, bytes_total: u32) -> u128 {
    println!("Thread <{}>: Beginning a test with {} queries ({} bytes total)", 
        thread_id, queries, bytes_total);
    let start_ts = Instant::now();

    thread::sleep(Duration::from_secs(2));
    let working_time_ms = start_ts.elapsed().as_millis();
    println!("Thread <{}> Finished", thread_id);
    working_time_ms
}

fn load_test_data(path: &Path) -> Result<Vec<TestDocument>, csv::Error> {
    let mut reader = ReaderBuilder::new()
        .has_headers(false)
        .from_path(path)?;
    reader.deserialize()
        .map(|result: Result<TestDocument, csv::Error>| result)
        .collect()
}