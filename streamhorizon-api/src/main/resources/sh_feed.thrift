namespace java com.threeglav.sh.bauk.rpc   
      
typedef i32 int
typedef i64 long

struct InputFeed {
  1: string feedName,
  2: long lastModifiedTimestamp,
  3: long sizeBytes,
  4: list<string> data
}

enum ProcessingResult {
  SUCCESS = 1,
  INVALID_FEED = 2,
  PROCESSING_ERROR = 3,
  UNKNOWN = 4
}
      
service SHFeedProcessor {  // defines the service to add two numbers  
	ProcessingResult processFeed(1:InputFeed feedToProcess),  
}  