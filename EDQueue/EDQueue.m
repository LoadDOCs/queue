//
//  EDQueue.m
//  queue
//
//  Created by Andrew Sliwinski on 6/29/12.
//  Copyright (c) 2012 Andrew Sliwinski. All rights reserved.
//

#import "EDQueue.h"
#import "EDQueueStorageEngine.h"

@interface EDQueue ()
{
    BOOL _isRunning;
    NSUInteger _retryLimit;
    dispatch_queue_t _queue;
}

@property (nonatomic) EDQueueStorageEngine *engine;
@property (nonatomic, readwrite) NSString *activeTask;

@end

//

@implementation EDQueue

@synthesize isRunning = _isRunning;
@synthesize retryLimit = _retryLimit;

#pragma mark - Singleton

+ (EDQueue *)sharedInstance
{
    static EDQueue *singleton = nil;
    static dispatch_once_t once = 0;
    dispatch_once(&once, ^{
        singleton = [[self alloc] init];
    });
    return singleton;
}

#pragma mark - Init

- (id)init
{
    self = [super init];
    if (self) {
        _engine = [[EDQueueStorageEngine alloc] init];
        _queue = dispatch_queue_create("EDQueue", NULL);
        _retryLimit = 4;
    }
    return self;
}

- (void)dealloc
{    
    self.delegate = nil;
    _engine = nil;
}

#pragma mark - Public methods

/**
 * Adds a new job to the queue.
 *
 * @param {id} Data
 * @param {NSString} Task label
 *
 * @return {void}
 */
- (void)enqueueWithData:(id)data forTask:(NSString *)task
{
    if (data == nil) data = @{};
    [self.engine createJob:data forTask:task];
    [self tick];
}

/**
 * Returns true if a job exists for this task.
 *
 * @param {NSString} Task label
 *
 * @return {Boolean}
 */
- (BOOL)jobExistsForTask:(NSString *)task
{
    BOOL jobExists = [self.engine jobExistsForTask:task];
    return jobExists;
}

/**
 * Returns true if the active job if for this task.
 *
 * @param {NSString} Task label
 *
 * @return {Boolean}
 */
- (BOOL)jobIsActiveForTask:(NSString *)task
{
    BOOL jobIsActive = [self.activeTask length] > 0 && [self.activeTask isEqualToString:task];
    return jobIsActive;
}

/**
 * Returns the list of jobs for this 
 *
 * @param {NSString} Task label
 *
 * @return {NSArray}
 */
- (NSDictionary *)nextJobForTask:(NSString *)task
{
    NSDictionary *nextJobForTask = [self.engine fetchJobForTask:task];
    return nextJobForTask;
}

- (NSArray *) getAllJobs{
  return [self.engine getAllJobs];
}

/**
 * Starts the queue.
 *
 * @return {void}
 */
- (void)start
{
    if (!self.isRunning) {
        _isRunning = YES;
        [self tick];
    }
}

/**
 * Stops the queue.
 * @note Jobs that have already started will continue to process even after stop has been called.
 *
 * @return {void}
 */
- (void)stop
{
    if (self.isRunning) {
        _isRunning = NO;
    }
}



/**
 * Empties the queue.
 * @note Jobs that have already started will continue to process even after empty has been called.
 *
 * @return {void}
 */
- (void)empty
{
    [self.engine removeAllJobs];
}


#pragma mark - Private methods

/**
 * Checks the queue for available jobs, sends them to the processor delegate, and then handles the response.
 *
 * @return {void}
 */
- (void)tick
{
    dispatch_async(_queue, ^{
        if (self.isRunning && [self.engine fetchJobCount] > 0) {
            id job = [self.engine fetchJob];
            self.activeTask = [(NSDictionary *)job objectForKey:@"task"];
            
            // Pass job to delegate
            if ([self.delegate respondsToSelector:@selector(queue:processJob:completion:)]) {
                [self.delegate queue:self processJob:job completion:^(EDQueueResult result) {
                    [self processJob:job withResult:result];
                    self.activeTask = nil;
                }];
            } else {
                EDQueueResult result = [self.delegate queue:self processJob:job];
                [self processJob:job withResult:result];
                self.activeTask = nil;
            }
        }
    });
}

- (void)processJob:(NSDictionary*)job withResult:(EDQueueResult)result
{
    // Check result
    switch (result) {
        case EDQueueResultSuccess: {
            [self.engine removeJob:[job objectForKey:@"id"]];
            break;
        }
        case EDQueueResultFail: {
            NSUInteger currentAttempt = [[job objectForKey:@"attempts"] intValue] + 1;
            if (currentAttempt < self.retryLimit) {
                [self.engine incrementAttemptForJob:[job objectForKey:@"id"]];
            } else {
                [self.engine removeJob:[job objectForKey:@"id"]];
            }
            break;
        }
        case EDQueueResultCritical: {
            [self errorWithMessage:@"Critical error. Job canceled."];
            [self.engine removeJob:[job objectForKey:@"id"]];
            break;
        }
    }

    // Drain
    if ([self.engine fetchJobCount] > 0) {
        [self tick];
    }
}

/**
 * Writes an error message to the log.
 *
 * @param {NSString} Message
 *
 * @return {void}
 */
- (void)errorWithMessage:(NSString *)message
{
    NSLog(@"EDQueue Error: %@", message);
}

@end
