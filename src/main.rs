extern crate core_affinity;

mod db;
mod queue;
mod transcoder;
mod bucket_allocator;

use db::DB;
use queue::JobQueue;
use transcoder::transcoder;
use bucket_allocator::BucketAllocator;


use dotenvy::dotenv;
use mimalloc::MiMalloc;

#[global_allocator]
static GLOBAL: MiMalloc = MiMalloc;


/*
__config_params__

WORKER__COUNT = usize
QUEUE__CAPACITY = usize
STORAGE__BUCKET__LIMIT = usize
MAX__JOB__FETCH__ATTEMPT = usize
OUTPUT__DIRECTORY__PATH = str

// Database config
DB__PG__HOST = str
DB__PG__USER = str 
DB__PG__PASSWORD = str
DB__PG__DBNAME = str
DB__PG__POOL__MAX_SIZE = usize
DB__PG__POOL__TIMEOUTS__WAIT__SECS = usize
DB__PG__POOL__TIMEOUTS__WAIT__NANOS = usize

*/


fn main() {
    dotenv().ok();

    let db = DB::initalize()?;
    let queue = JobQueue::new(1000, db, 5);

    let bucket_allocator = BucketAllocator::new(10000, String::from("/")).expect("Failed");
    bucket_allocator.scan_directory();


    let args: Vec<String> = std::env::args().collect();
    if args.len() != 3 {
        eprintln!("Usage: {} <input> <output>", args[0]);
        std::process::exit(1);
    }

    transcoder(&args[1], &args[2], "")?;
    println!("Transcoding complete: {} -> {}", &args[1], &args[2]);
    Ok(())


    /*
        let max_count: usize = 10000;
    let output_directory: String = String::from('c');

    let mut bucket_allocator: BucketAllocator = BucketAllocator::new(max_count, output_directory).expect("Failed");

    let allocated_bucket = bucket_allocator.allocate_bucket();
    
    if let Ok(bucket) = allocated_bucket {
        println!("{}", bucket.to_string_lossy());
    }
     */
}
