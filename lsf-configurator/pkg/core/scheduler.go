package core

import (
	"context"
	"fmt"
	"log"
	"sync"
)

// Task is a unit of work run by a worker pool; a task that returns an error is retried.
type Task func() (interface{}, error)

type taskInternal struct {
	execute    Task
	retries    int
	maxRetries int
	resultChan chan Result
}

// Result is the outcome of a task: its value, or the error of its last attempt.
type Result struct {
	Value interface{}
	Err   error
}

// WorkerPool runs tasks on a fixed number of workers and retries failed tasks up to their retry limit.
type WorkerPool struct {
	taskQueue chan taskInternal
	wg        sync.WaitGroup
	once      sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
}

// Scheduler runs tasks asynchronously and delivers their results on channels.
type Scheduler interface {
	AddTask(task Task, maxRetries int) <-chan Result
	Close()
}

// NewScheduler starts a worker pool with numWorkers workers and a task queue of length queueSize.
func NewScheduler(numWorkers int, queueSize int) Scheduler {
	ctx, cancel := context.WithCancel(context.Background())
	w := WorkerPool{
		taskQueue: make(chan taskInternal, queueSize),
		ctx:       ctx,
		cancel:    cancel,
	}

	for i := 0; i < numWorkers; i++ {
		w.wg.Add(1)
		go w.worker()
	}

	return &w
}

// AddTask queues a task and returns a channel that delivers its result once it succeeds or runs out of retries. If
// the pool shuts down before the task is queued, the channel delivers an error.
func (w *WorkerPool) AddTask(task Task, maxRetries int) <-chan Result {
	resultChan := make(chan Result, 1)
	go func() {
		select {
		case <-w.ctx.Done():
			resultChan <- Result{Err: fmt.Errorf("thread pool shutting down")}
			close(resultChan)
		case w.taskQueue <- taskInternal{
			execute:    task,
			retries:    0,
			maxRetries: maxRetries,
			resultChan: resultChan,
		}:
		}
	}()
	return resultChan
}

// Close stops the worker pool and waits for its workers to exit.
func (w *WorkerPool) Close() {
	w.once.Do(func() {
		w.cancel()
		close(w.taskQueue)
	})
	w.wg.Wait()
}

func (w *WorkerPool) worker() {
	defer w.wg.Done()

	for {
		select {
		case task, ok := <-w.taskQueue:
			if !ok {
				return
			}
			result, err := task.execute()
			if err != nil {
				// Task failed, check if we should retry
				if task.retries < task.maxRetries {
					task.retries++
					log.Printf("Task failed with error,: %s retrying... (attempt %d/%d)", err, task.retries, task.maxRetries)

					select {
					case w.taskQueue <- task:
					case <-w.ctx.Done():
						return
					}
				} else {
					// Max retries reached, send the result
					task.resultChan <- Result{Value: nil, Err: err}
					close(task.resultChan)
				}
			} else {
				// Task succeeded, send the result
				task.resultChan <- Result{Value: result, Err: nil}
				close(task.resultChan)
			}
		case <-w.ctx.Done():
			return
		}
	}
}
