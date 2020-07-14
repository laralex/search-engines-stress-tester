extern crate clap;
use clap::{Arg, App};
use url::Url;

#[derive(Debug)]
enum Engine {
    Meilisearch(Url),
    Typesense(Url),
}

#[derive(Debug)]
struct StressTestParams {
    queries_per_sec: u16,
    bytes_per_sec: u32,
    threads: u16,
}

#[derive(Debug)]
enum Action {
    StressTest(StressTestParams),
    Ping,
    Purge,
}

fn ping(engine: Engine) {
    print!("ping {:?}", engine);
}

fn purge(engine: Engine) {
    print!("purge {:?}", engine);
}

fn stress_test(engine: Engine, stress_params: StressTestParams) {
    print!("stress_test e: {:?}, p: {:?}", engine, stress_params);
}

fn send_query() {

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
        // .arg(Arg::with_name("typesense")
        //     .about(concat!("Selected server is running Typesense engine ", 
        //                   "(it should be launched separetely)")))
        // .group(ArgGroup::with_name("engine")
        //     .args(&["meilisearch", "typesense"])
        //     .required(true))

        .arg(Arg::with_name("alternative_action")
            .possible_values(&["ping", "PURGE"])
            .about(concat!("An action instead of stress testing the engine ",
                        "(ping to test connection, PURGE to delete all the data from server))")))
        // .arg(Arg::with_name("PURGE")
        //     .about(concat!("Instead of testing the engine, its data will be completely deleted ",
        //                 "(helps to make a clean test)")))
        // .group(ArgGroup::with_name("action")
        //     .args(&["ping", "PURGE"]))
        // flags
        .arg(Arg::with_name("queries_per_sec")
            .short('q')
            .long("queries")
            .takes_value(true)
            .value_name("Q_PER_SEC")
            .default_value("10")
            .about("How many queries to send to the selected engine per second"))
        .arg(Arg::with_name("bytes_per_sec")
            .short('b')
            .long("bytes")
            .takes_value(true)
            .value_name("B_PER_SEC")
            .default_value("1024")
            .about(concat!("How many bytes to send to the selected engine per second on average ",
                          "(each query sends bytes from formula: BYTES_PER_SEC/QUERIES_PER_SEC)")))
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
        Some("ping") => ping(engine),
        Some("PURGE") => purge(engine),
        None => stress_test(engine, StressTestParams {
            queries_per_sec: matches.value_of("queries_per_sec")
                .expect("Improper config: queries per sec")
                .parse()
                .expect("--queries value is not a number"),
            bytes_per_sec: matches.value_of("bytes_per_sec")
                .expect("Improper config: bytes per sec")
                .parse().
                expect("--bytes value is not a number"),
            threads: matches.value_of("threads")
                .expect("Improper config: threads number")
                .parse()
                .expect("--threads value is not a number"),
        }),
        _ => panic!("Improper config: alternative action")
    };
    //print!("{:?}", matches);
}
