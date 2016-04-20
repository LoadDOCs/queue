//
//  OrderByAttemptTests.m
//  OrderByAttemptTests
//
//  Created by Peter Pong on 4/25/16.
//  Copyright © 2016 DIY, Co. All rights reserved.
//

#import <XCTest/XCTest.h>
#import "EDQueue.h"

@interface OrderByAttemptTests : XCTestCase<EDQueueDelegate> {

@private NSMutableArray *_resultArray;

}
@end

@implementation OrderByAttemptTests


/**
    We test that the jobs are ordered by the number of attempts
    
    For this test, I include a parameter numRequiredFails in the job itself. 
    When the job gets run, I only issue a EDQueueResultSuccess when the 
    number of attempts is equal to the numRequiredFails.
 
    I set the number of numRequiredFails so that the resultArray of jobIds
    is in descending order
 
    e.g. if numJobs is set to 100
    job id 0 has 100 number required fails
    job id 1 has 99 number required fails
    job id 2 has 98 number required fails
    ...
    job id 99 has 1 required fail
 
    etc
 
    The resultArray should like [99, 98, ..., 1, 0]
 **/

-(void) testOrderByAttempts {
    int numJobs = 100;
    EDQueue * queue = [EDQueue sharedInstance];
    queue.retryLimit = numJobs + 1;
    
    _resultArray = [[NSMutableArray alloc] initWithCapacity:numJobs];
    
    [self queueJobs:numJobs];
    
    while ([_resultArray count] != numJobs);
    
    // Verify that the result array is equal
    for (int i = 0; i < numJobs; i++) {
        XCTAssertEqual([[_resultArray objectAtIndex:i] intValue], (numJobs - i -1), @"Tasks are not in Order");
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
        NSNumber *numRequiredFails = [NSNumber numberWithInteger: (numJobs - i)];
        NSMutableDictionary *job = [[NSMutableDictionary alloc] initWithObjectsAndKeys:[NSNumber numberWithInteger:i], @"id", numRequiredFails, @"numRequiredFails", nil];
        
        XCTestExpectation *completionExpectation = [self expectationWithDescription:@"Queueing...Please Wait"];
        [self queueJob:job callback:^(NSString *taskId) {
            [completionExpectation fulfill];
        }];
        [self waitForExpectationsWithTimeout:10 handler:nil];
    }
}


- (EDQueueResult)queue:(EDQueue *)queue processJob:(NSDictionary *)job {
    NSDictionary * data = [job objectForKey:@"data"];
    
    NSNumber *taskId = [data objectForKey: @"id"];
    
    NSLog(@"processing job: %d", [taskId intValue]);
    
    int attempts = [ (NSNumber *)[job objectForKey:@"attempts"] intValue];
    int numRequiredFails = [[data objectForKey:@"numRequiredFails"] intValue];
    
    if (attempts != numRequiredFails) {
        return EDQueueResultFail;
    } else {
        [_resultArray addObject:taskId];
        return EDQueueResultSuccess;
    }
}

- (void)setUp {
    [super setUp];
    
    _resultArray = [[NSMutableArray alloc] init];
    
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
    
    [_resultArray removeAllObjects];
    _resultArray = nil;
}


@end

