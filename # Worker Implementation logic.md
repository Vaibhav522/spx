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