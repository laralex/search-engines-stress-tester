extern crate clap;
//extern crate tokio;
extern crate url;
extern crate num;
extern crate reqwest;

use clap::{Arg, App};
//use tokio::prelude::*;
use url::Url;
use num::Integer;
use reqwest::{blocking, Error};

use std::thread;
use std::sync::Arc;
use std::time::{Duration, Instant};

#[derive(Debug, Clone)]
enum Engine {
    Meilisearch(Url),
    Typesense(Url),
}

#[derive(Debug, Clone)]
struct StressTestParams {
    queries_total: u16,
    bytes_total: u32,
    threads_number: u16,
}

#[derive(Debug)]
enum Action {
    StressTest(StressTestParams),
    Ping,
    Purge,
}

fn handle_ping(engine: Engine) {
    println!(">>> Pinging {:?}", engine);
    match ping(engine) {
        Ok(_) => println!("Pinging successfully!"),
        Err(e) => println!("Ping failed: {:?}", e),
    };
}

fn ping(engine: Engine) -> Result<String, Error> {
    let ping_dest = match engine {
        Engine::Meilisearch(url) => { 
            url.join("/indexes").unwrap()
        },
        Engine::Typesense(url) => {
            url.join("/health").unwrap()
        },
        _ => panic!("Unsupported engine"),
    };
    let response = reqwest::blocking::get(ping_dest)?.text()?;
    Ok(response)
}

fn handle_purge(engine: Engine) {
    print!("purge {:?}", engine);
}

fn handle_stress_test(engine: Engine, stress_params: StressTestParams) {
    assert!(stress_params.threads_number != 0, "Given threads number is 0");
    assert!(stress_params.queries_total != 0, "Given queries number is 0");
    assert!(stress_params.bytes_total != 0, "Given bytes number is 0");
    
    println!(" >>> Stress testing\nSearch engine: {:?}\nQueries: {}\nBytes: {}\nThreads number: {}", 
        engine, stress_params.queries_total, stress_params.bytes_total, stress_params.threads_number);
    let mut threads = vec![];
    println!(" >>> Beginning...\nLaunching {} stress testing threads", stress_params.threads_number);
    let start_ts = Instant::now();
    for t in 0..stress_params.threads_number {
        let engine_thread_local = engine.clone();
        let mut stress_params_thread_local = StressTestParams{
            queries_total: (stress_params.queries_total - 1).div_floor(&(stress_params.threads_number)) + 1, // divide with ceiling
            bytes_total: (stress_params.bytes_total - 1).div_floor(&(stress_params.threads_number as u32)) + 1,
            threads_number: 1,
        };
        threads.push(
            thread::spawn(move || thread_stress_test(t, engine_thread_local, stress_params_thread_local))
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
    println!(" >>> Stress testing is done!\nSuccessfully finished threads: {}\nUnsuccessfully finished threads: {}", 
        stress_params.threads_number - unfinished_threads_cnt, unfinished_threads_cnt);
    println!("Test duration: {} ms\nThreads work duration: {} ms", test_duration.as_millis(), threads_total_time_ms);
}

fn thread_stress_test(thread_id: u16, engine: Engine, stress_params: StressTestParams) -> u128 {
    println!("Thread <{}> testing by sending {} queries ({} bytes total)", 
        thread_id, stress_params.queries_total, stress_params.bytes_total);
    let start_ts = Instant::now();
    thread::sleep(Duration::from_secs(2));
    let working_time_ms = start_ts.elapsed().as_millis();
    println!("Thread <{}> Finished", thread_id);
    working_time_ms
}

fn main() {
    let matches = App::new(concat!("Load tester for document search engines ", 
                                       "(Meilisearch and Typesense)"))
        .version(env!("CARGO_PKG_VERSION"))
        //options
        .arg(Arg::with_name("engine")
            .possible_values(&["meilisearch", "typesense"])
            .about(concat!("Selected server to interact with ", 
                          "(it should be launched separetely)")))
        .arg(Arg::with_name("alternative_action")
            .possible_values(&["ping", "PURGE"])
            .about(concat!("An action instead of stress testing the engine ",
                        "(ping to test connection, PURGE to delete all the data from server))")))
        // flags
        .arg(Arg::with_name("queries_total")
            .short('q')
            .long("queries")
            .takes_value(true)
            .value_name("QUERIES")
            .default_value("10")
            .about("How many queries to send to the selected engine"))
        .arg(Arg::with_name("bytes_total")
            .short('b')
            .long("bytes")
            .takes_value(true)
            .value_name("BYTES")
            .default_value("1024")
            .about(concat!("How many bytes to send to the selected engine (approx)",
                          "(each query sends bytes from formula: bytes_total/queries_total)")))
        .arg(Arg::with_name("engine_url")
            .short('u')
            .long("url")
            .takes_value(true)
            .value_name("ENGINE_URL")
            .default_value("http://localhost")
            .about("Selected search engine's URL"))
        .arg(Arg::with_name("engine_port")
            .short('p')
            .long("port")
            .takes_value(true)
            .value_name("ENGINE_PORT")
            .default_value_if("engine", Some("meilisearch"), "7700")
            .default_value_if("engine", Some("typesense"), "8108")
            .about(concat!("Selected search engine's port number ", 
                          "[default: 7700 for Meilisearch, 8108 for Typesense]")))
        .arg(Arg::with_name("threads")
            .short('t')
            .long("threads")
            .takes_value(true)
            .value_name("THREADS")
            .default_value("1")
            .about(concat!("Threads number for this app (one thread might not manage ", 
                   "to send all the queries in time, in that case use more threads)")))
        .get_matches();  

    let mut engine_url = Url::parse(matches.value_of("engine_url")
            .expect("Improper config: engine url"))
        .expect("Improper Engine URL given");
    engine_url.set_port(Some(matches.value_of("engine_port")
                .expect("Improper config: port number")
                .parse()
                .expect("Engine Port is not a number")))
        .expect("Engine Port is an invalid port number");
    let engine = match matches.value_of("engine") {
        Some("meilisearch") => Engine::Meilisearch(engine_url),
        Some("typesense") => Engine::Typesense(engine_url),
        _ => panic!("Improper config: engine"),
    };

    match matches.value_of("alternative_action") {
        Some("ping") => handle_ping(engine),
        Some("PURGE") => handle_purge(engine),
        None => handle_stress_test(engine, StressTestParams {
            queries_total: matches.value_of("queries_total")
                .expect("Improper config: queries")
                .parse()
                .expect("--queries value is not an unsigned number"),
            bytes_total: matches.value_of("bytes_total")
                .expect("Improper config: bytes")
                .parse().
                expect("--bytes value is not an unsigned number"),
            threads_number: matches.value_of("threads")
                .expect("Improper config: threads number")
                .parse()
                .expect("--threads value is not an unsigned number"),
        }),
        _ => panic!("Improper config: alternative action")
    };
    //print!("{:?}", matches);
}
