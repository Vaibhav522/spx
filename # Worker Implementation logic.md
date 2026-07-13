# Worker Implementation logic


This whole system doesn't handle external logic, 

1. Fetch jobs from server in batches
2. Enqueue fetched jobs
    1. Pop one job at a time
    2. Transcode and write to disk
3. Mark job completed to db, if no error else provide the erorr faced to db


Job fetch result:

``` js
[
    {
        file_sha: str,
        file_name: str,
        file_path: str
    }
]
```


Constructing destination_file_path

``` pseudocode
1. Given file_name of type string
2. And  knowing output format of .pcm
3. Call bucket allocator to allocate a bucket_id

    file_destination = <output_directory>/<bucket_id>/<filename>.pcm
```



Bucket Allocator: Given a `max_content_count` bucket allocators job is to allocate a folder such that `total_file_or_folder_count <= max_content_count`. To do this, we need to track list of all 1st level folders inside `output_directory`. Since, we have single node, we don't need external source of truth of this. 

1. Scan the output directory for all the first level directories
2. Filter anything that has `total_file_or_folder_count > max_content_count`
3. Mark pick one of the directories and allocate the directory.


In-memory allocator state → authoritative during normal operation.
The Standard Approach: `Arc<Mutex<T>>` A Mutex `<T>` (Mutual Exclusion) ensures that exactly one thread can access and mutate the underlying data at any given second. To share this mutex among multiple threads, you must wrap it in an Arc<T> (Atomic Reference Counted) pointer, which allows safe, shared ownership across thread boundaries. 


1. Scan the output directory and create an in-memory table



#### Processed update to DB: 

``` js
{
    file_sha: str,
    file_destination: str,
    output_file_size: int (bytes),
    transcoder_error_faced: {
        error_type: int,
        error_body: str
    },
    processed_at: datetime,
    time_took_to_proces: float (sec)
}
```



##  When workers fail and we have to manage temp files.

In linux when `PrivateTmp=true`. When this is enabled, the system creates a unique, isolated, hidden temporary directory specifically for that process. To the process, it looks like it's writing to /tmp, but no other process on the machine can see it.


A single transaction pull from the server. 




a single transactional data pull from the db.

keep attempt count to 2.


``` sql



```


Create an shared db pool size of 16.

db config is sourced from .env file present at the "." folder of execution.



intitalize db connection config, once complete start a connection pool.



The objective is to keep the db fetch and queue functions to be seperated, meaning the queue should only have access to fetch job's and nothing else.
This keeps thing clean as we want. But how can we do that? 

Now, given access to 



The queue is intialized externally, which is consumed by multiple threads, meaning following:

initalized queue object shared with thread so we can do `Arc::new(Queue::new(*args));` with atomic reference counter we clone for each thread, such that each thread can safely consume the queue. I am using `crossbeam-queue` which is thread safe, and non-blocking ensuring workers aren't kept idle. Now, fields that will be actively called by threads are as follow: `requested_job_pull: Atomic Boolean` this is to ensure only one thread can change it, next we have source field, depends upon `requeusted_job_pull` to be false to be called, but since this instance can be called from any thread we want to wrap that with `Arc` too such that it's a thread safe call.


Now, to call job_pull, the `requested_job_pull` value should be false, and the `max_job_fetch_attempt > failed_attempt` and `queue should have 30 % left`, now when such conditions are met, we set the atomic bool to true, and make a fetch request, if failed 
whatever the condition is we know the 


the fetcher is a external thread, meaning we have to pass mutable references to the data fields, allowing it to mutate it. 

