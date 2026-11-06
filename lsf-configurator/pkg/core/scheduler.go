package core

import (
	"context"
	"fmt"
	"log"
	"sync"
)

type Task func() (interface{}, error)

type taskInternal struct {
	execute    Task
	retries    int
	maxRetries int
	resultChan chan Result
}

type Result struct {
	Value interface{}
	Err   error
}

type WorkerPool struct {
	taskQueue chan taskInternal
	wg        sync.WaitGroup
	once      sync.Once
	ctx       context.Context
	cancel    context.CancelFunc
}

type Scheduler interface {
	AddTask(task Task, maxRetries int) <-chan Result
	Close()
}

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
