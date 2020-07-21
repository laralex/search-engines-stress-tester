#![allow(dead_code)]
#![allow(unused_imports)]

use clap::{Arg, App, ArgMatches};
use reqwest::Url;

use std::path::Path;

mod actions;
mod typesense;
mod meilisearch;
mod data;

fn parse_args() -> ArgMatches {
    App::new(concat!("Load tester for document search engines ", 
                                       "(Meilisearch and Typesense)"))
        .version(env!("CARGO_PKG_VERSION"))
        //options
        .arg(Arg::with_name("engine")
            .possible_values(&["meilisearch", "typesense"])
            .about(concat!("Selected server to interact with ", 
                          "(it should be launched separetely)")))
        .arg(Arg::with_name("alternative_action")
            .possible_values(&["ping", "PURGE"])
            .conflicts_with_all(&["queries_total", "initial_documents", "test_data_path"])
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
        .arg(Arg::with_name("initial_documents")
            .short('i')
            .long("initial-docs")
            .takes_value(true)
            .value_name("DOCUMENTS_NUMBER")
            .default_value("10000")
            .about(concat!("How many initial documents to send for a test")))
        // .arg(Arg::with_name("bytes_total")
        //     .short('b')
        //     .long("bytes")
        //     .takes_value(true)
        //     .value_name("BYTES")
        //     .default_value("1024")
        //     .about(concat!("How many bytes to send to the selected engine (approx)",
        //                   "(each query sends bytes from formula: bytes_total/queries_total)")))
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
        // .arg(Arg::with_name("threads")
        //     .short('t')
        //     .long("threads")
        //     .takes_value(true)
        //     .value_name("THREADS")
        //     .default_value("1")
        //     .about(concat!("Threads number for this app (one thread might not manage ", 
        //            "to send all the queries in time, in that case use more threads)")))
        .arg(Arg::with_name("api_key")
            .short('k')
            .long("api-key")
            .required_if("engine","typesense")
            .value_name("API_KEY")
            .takes_value(true)
            .about("Typesense's API key of the running server"))
        .arg(Arg::with_name("test_data_path")
            .short('d')
            .long("test-data")
            .required_unless("alternative_action")
            //.required_if("alternative_action", "")
            .value_name("PATH")
            .takes_value(true)
            .about("Path to a CSV file with stress test data. It will be duplicated up to required size."))
        .get_matches()
}
#[tokio::main]
async fn main() -> Result<(), Box<dyn std::error::Error>> {
    let matches = parse_args();  

    let mut engine_url = Url::parse(matches.value_of("engine_url")
            .expect("Improper config: engine url"))
        .expect("Improper Engine URL given");
    engine_url.set_port(Some(matches.value_of("engine_port")
                .expect("Improper config: port number")
                .parse()
                .expect("Engine Port is not a number")))
        .expect("Engine Port is an invalid port number");
        
    let engine = match matches.value_of("engine") {
        Some("meilisearch") => actions::Engine::Meilisearch(engine_url),
        Some("typesense") => actions::Engine::Typesense(engine_url, 
            matches.value_of("api_key")
                .expect("Improper config: typesense API key")
                .to_owned()),
        _ => panic!("Improper config: engine"),
    };

    match matches.value_of("alternative_action") {
        Some("ping") => actions::handle_ping(engine).await,
        Some("PURGE") => actions::handle_purge(engine).await,
        None => actions::handle_stress_test(engine, actions::StressTestParams {
            queries_total: matches.value_of("queries_total")
                .expect("Improper config: queries")
                .parse()
                .expect("--queries value is not an unsigned number"),
            initial_documents: matches.value_of("initial_documents")
                .expect("Improper config: initial_documents")
                .parse()
                .expect("--initial-docs value is not an unsigned number"),
            // bytes_total: matches.value_of("bytes_total")
            //     .expect("Improper config: bytes")
            //     .parse().
            //     expect("--bytes value is not an unsigned number"),
            // threads_number: matches.value_of("threads")
            //     .expect("Improper config: threads number")
            //     .parse()
            //     .expect("--threads value is not an unsigned number"),
            data_path: { 
                let path = Path::new(matches
                        .value_of("test_data_path")
                        .expect("Improper config: test_data_path"));
                if let None = path.file_name() {
                    panic!("--test-data is not a file path");
                }
                if !path.exists() {
                    panic!("--test-data path doesn't exist");
                }
                path
            },
        })
        .await,
        _ => panic!("Improper config: alternative action")
    };
    Ok( () )
}
