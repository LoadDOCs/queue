//
//  queueTests.m
//  queueTests
//
//  Created by Peter Pong on 4/25/16.
//  Copyright © 2016 DIY, Co. All rights reserved.
//

#import <XCTest/XCTest.h>
#import "EDQueue.h"

@interface queueTests : XCTestCase<EDQueueDelegate>

@end

@implementation queueTests

NSMutableArray *_semaphoreArray;
NSMutableArray *_resultArr;

int numJobs = 100;

- (void)setUp {
    [super setUp];

    _semaphoreArray = [[NSMutableArray alloc] init];
    _resultArr = [[NSMutableArray alloc] init];

    EDQueue * queue = [EDQueue sharedInstance];
    queue.delegate = self;

    [queue empty];
    [queue start];
}

- (void)tearDown {
  // Put teardown code here. This method is called after the invocation of each test method in the class.
    [super tearDown];
    EDQueue * queue = [EDQueue sharedInstance];
    [queue empty];
    [_semaphoreArray  removeAllObjects];
    [_resultArr removeAllObjects];
}

/**
 Test only one job gets run at a time
 **/
-(void) testJobsRunOneAtATime {
    _semaphoreArray = [[NSMutableArray alloc] initWithCapacity:numJobs];
    _resultArr = [[NSMutableArray alloc] initWithCapacity:numJobs];
    
    [self queueJobs:numJobs];
    [self runJobsInRandomOrder: numJobs];
    
    // Verify that the result array is equal
    for (int i = 0; i < numJobs; i++) {
        XCTAssertEqual([[_resultArr objectAtIndex:i] intValue], i, @"tasks are not in order");
    }
    XCTAssertEqual([[[EDQueue sharedInstance] getAllJobs] count], 0, @"Task Queue is not empty");
}

/**
 Fetch job respects the ordering of id and attempts. Attempts is the primary
 **/


-(void) queueJob:(NSMutableDictionary*) job callback: (void (^)(NSString *))callbackBlock {
    EDQueue * queue = [EDQueue sharedInstance];
    int taskId = [[job objectForKey:@"id"] intValue];
    NSString * taskIdString = [NSString stringWithFormat:@"task%d", taskId];
    [queue enqueueWithData:job forTask:taskIdString];
    
    callbackBlock([job objectForKey:@"taskId"]);
}

-(void) queueJobs:(int) numJobs {
    for (int i = 0; i < numJobs; i++) {
        dispatch_semaphore_t sem = dispatch_semaphore_create(0);
        
        [_semaphoreArray addObject:sem];
        NSMutableDictionary *job = [[NSMutableDictionary alloc] initWithObjectsAndKeys:[NSNumber numberWithInteger:i], @"id", nil];
        
        XCTestExpectation *completionExpectation = [self expectationWithDescription:@"Queueing...Please Wait"];
        [self queueJob:job callback:^(NSString *taskId) {
            [completionExpectation fulfill];
        }];
        [self waitForExpectationsWithTimeout:10 handler:nil];
    }
}


-(void) runJobsInRandomOrder: (int) numJobs{
    EDQueue * queue = [EDQueue sharedInstance];

    int numIterations = 0;
    
    while ([[queue getAllJobs] count] > 0) {
        int randomIndex = arc4random_uniform(numJobs);
        dispatch_semaphore_t sem = [_semaphoreArray objectAtIndex:randomIndex];
        dispatch_semaphore_signal(sem);
        numIterations++;
    }
    NSLog(@"it took %d iterations for all the jobs to run", numIterations);
}


- (EDQueueResult)queue:(EDQueue *)queue processJob:(NSDictionary *)job {
    NSDictionary * data = [job objectForKey:@"data"];
    
    NSNumber *taskId = [data objectForKey: @"id"];
    
    dispatch_semaphore_t sem = [_semaphoreArray objectAtIndex:[taskId intValue]];
    NSLog(@"semaphore: %d is waiting", [taskId intValue]);

    dispatch_semaphore_wait(sem, DISPATCH_TIME_FOREVER);
    
    NSLog(@"processing job: %d", [taskId intValue]);

    [_resultArr addObject:taskId];
    
    return EDQueueResultSuccess;
}



@end

