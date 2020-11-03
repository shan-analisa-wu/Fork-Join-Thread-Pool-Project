#include "threadpool.h"
#include <stdbool.h>
#include <semaphore.h>
#include <pthread.h>
#include "list.h"
#include <stdlib.h>
#include <stdio.h>

/**
 * threadpool.c
 *
 * A work-stealing, fork-join thread pool.
 */

/*
 * Define thread_pool structure and future structure
 */
typedef struct thread_pool{
    bool shutDown;
    struct list tasks;
    int numThread;

    //the pid of the threads in the thread pool
    pthread_t* threads;
    pthread_mutex_t pool_lock;
    sem_t pool_empty;
}thread_pool;

typedef struct future{
    fork_join_task_t task;
    void* args;
    void* result;
    sem_t future_done;
    struct list_elem elem;

    //a pointer that points back to the thread pool
    thread_pool *ptrpool;

    //the status of the future
    int status;
    //pthread_mutex_t future_lock;
}future;
static void* thread_execute(void* pool);

/* Create a new thread pool with no more than n threads. */
struct thread_pool * thread_pool_new(int nthreads){
    thread_pool* newPool = malloc(1 * sizeof(thread_pool));
    newPool->threads = malloc(nthreads * sizeof(pthread_t));
    newPool->numThread = nthreads;
    newPool->shutDown = false;
    pthread_mutex_init(&(newPool->pool_lock), NULL);
    list_init(&(newPool->tasks));
    sem_init(&newPool->pool_empty, 0, 0);

    //create the threads in the thread pool
    for(int i = 0; i < nthreads; i++){
        pthread_create(&(newPool->threads[i]), NULL, thread_execute, newPool);
    }
    return newPool;
}


/**
 * Each thread created execute this function
 */
static void* thread_execute(void* pool){
    thread_pool* curPool = (thread_pool*) pool;
    while(true){
        //wait until the thread pool is not empty
        sem_wait(&(curPool->pool_empty));
        pthread_mutex_lock(&(curPool->pool_lock));
        if(curPool->shutDown) {
            pthread_mutex_unlock(&(curPool->pool_lock));
            return NULL;
        }

        if(!list_empty(&(curPool->tasks))) {
            struct list_elem *task_elem = list_pop_front(&curPool->tasks);
            struct future *fut = list_entry(task_elem, struct future, elem);

            //change the status of the future to in progress
            fut->status = 1;
            pthread_mutex_unlock(&(curPool->pool_lock));
            fut->result = (*fut->task)(fut->ptrpool, fut->args);

            //change the status of the future to complete
            fut->status = 2;
            sem_post(&fut->future_done);
        }
        else {
            pthread_mutex_unlock(&(curPool->pool_lock));
        }

    }
}

/*
 * Shutdown this thread pool in an orderly fashion.
 * Tasks that have been submitted but not executed may or
 * may not be executed.
 *
 * Deallocate the thread pool object before returning.
 */
void thread_pool_shutdown_and_destroy(struct thread_pool *pool){
    pthread_mutex_lock(&(pool->pool_lock));
    pool->shutDown = true;
    pthread_mutex_unlock(&(pool->pool_lock));

    //let the thread stop waiting if the thread pool is shutting down
    for(int j = 0; j < pool->numThread; j++){
        sem_post(&pool->pool_empty);
    }

    for(int i = 0; i < pool->numThread; i++){
        pthread_join(pool->threads[i], NULL);
    }

    //destroy the locks and semaphore
    pthread_mutex_destroy(&pool->pool_lock);
    sem_destroy(&pool->pool_empty);

    free(pool->threads);
    free(pool);
}


/*
 * Submit a fork join task to the thread pool and return a
 * future.  The returned future can be used in future_get()
 * to obtain the result.
 * 'pool' - the pool to which to submit
 * 'task' - the task to be submitted.
 * 'data' - data to be passed to the task's function
 *
 * Returns a future representing this computation.
 */
struct future * thread_pool_submit(
        struct thread_pool *pool,
        fork_join_task_t task,
        void * data){
    //initialize a new future
    struct future* fut = malloc(1 * sizeof(struct future));
    fut->task = task;
    fut->args = data;

    //initialize the status of the future to incomplete
    fut->status = 0;
    fut->ptrpool = pool;
    sem_init(&fut->future_done, 0, 0);

    //push the future to global queue
    pthread_mutex_lock(&(pool->pool_lock));
    list_push_back(&(pool->tasks), &(fut->elem));
    sem_post(&pool->pool_empty);
    pthread_mutex_unlock(&(pool->pool_lock));

    return fut;
}

/* Make sure that the thread pool has completed the execution
 * of the fork join task this future represents.
 *
 * Returns the value returned by this task.
 */
void * future_get(struct future *fut){
    pthread_mutex_lock(&(fut->ptrpool->pool_lock));

    //if the future is completed or in progess, wait until
    //the future is complete and return the result
    if(fut->status == 1) {
        pthread_mutex_unlock(&(fut->ptrpool->pool_lock));

        sem_wait(&fut->future_done);

        return fut->result;
    }
    else if(fut->status == 2){
        pthread_mutex_unlock(&(fut->ptrpool->pool_lock));

        return fut->result;
    }
    else{
        //help processing the future if the future is incomplete
        fut->status = 1;
        list_remove(&(fut->elem));
        pthread_mutex_unlock(&(fut->ptrpool->pool_lock));

        fut->result = (*fut->task)(fut->ptrpool, fut->args);

        fut->status = 2;

        return fut->result;
    }
}

/* Deallocate this future.  Must be called after future_get() */
void future_free(struct future *fut){
    //free the future
    sem_destroy(&fut->future_done);
    free(fut);
}
