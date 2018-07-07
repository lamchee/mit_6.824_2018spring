package mapreduce

import (
	"fmt"
	"sync"
	"time"
)

//
// schedule() starts and waits for all tasks in the given phase (mapPhase
// or reducePhase). the mapFiles argument holds the names of the files that
// are the inputs to the map phase, one per map task. nReduce is the
// number of reduce tasks. the registerChan argument yields a stream
// of registered workers; each item is the worker's RPC address,
// suitable for passing to call(). registerChan will yield all
// existing registered workers (if any) and new ones as they register.
//
func schedule(jobName string, mapFiles []string, nReduce int, phase jobPhase, registerChan chan string) {
	var ntasks int
	var n_other int // number of inputs (for reduce) or outputs (for map)
	switch phase {
	case mapPhase:
		ntasks = len(mapFiles)
		n_other = nReduce
	case reducePhase:
		ntasks = nReduce
		n_other = len(mapFiles)
	}

	fmt.Printf("Schedule: %v %v tasks (%d I/Os)\n", ntasks, phase, n_other)

	// All ntasks tasks have to be scheduled on workers. Once all tasks
	// have completed successfully, schedule() should return.
	//
	// Your code here (Part III, Part IV).
	//

	taskChan := make(chan int)
	var wg sync.WaitGroup

	go func() {
		for i := 0; i < ntasks; i++ {
			wg.Add(1)
			taskChan <- i
		}
		wg.Wait()
		close(taskChan)
	}()

	for i := range taskChan {
		fmt.Printf("Schedule: Try to catch a worker, current task index : %d\n", i)
		worker := <-registerChan
		fmt.Printf("Schedule: Dispatch work, taskNumber : %d, worker : [%s]\n", i, worker)

		var args DoTaskArgs
		args.JobName = jobName
		args.NumOtherPhase = n_other
		args.Phase = phase
		args.TaskNumber = i
		args.File = mapFiles[i]
		go func(w string, args DoTaskArgs) {
			fmt.Printf("Schedule: Going to process job, worker : [%s], taskNumber : %d\n", worker, args.TaskNumber)
			if call(worker, "Worker.DoTask", args, nil) {
				go func(w string, task int) {
					defer wg.Done()
					ticker := time.NewTicker(1 * time.Second)
				loop:
					for j := 0; j < 3; j++ {
						fmt.Printf("Schedule: %d's time to wait put worker back to channel, taskNumber : %d, worker : %s\n", j, task, w)
						select {
						case registerChan <- w:
							fmt.Printf("Schedule: Put worker back to channel, taskNumber : %d, worker : %s \n", task, w)
							break loop
						case <-ticker.C:
							fmt.Printf("Schedule: Wait put worker back to channel timeout, taskNumber : %d, worker : %s\n", task, w)
						}
					}
					ticker.Stop()
				}(worker, args.TaskNumber)

				//registerChan <- worker
				fmt.Printf("Schedule: Job done! worker : [%s], taskNumber : %d\n", worker, args.TaskNumber)

			} else {
				fmt.Printf("Schedule: Put task back to channel, worker : [%s], taskNumber : %d\n", worker, args.TaskNumber)
				taskChan <- args.TaskNumber
				fmt.Printf("Schedule: Job failed! worker : [%s], taskNumber : %d\n", worker, args.TaskNumber)
			}
		}(worker, args)
	}

	ticker1 := time.NewTicker(2 * time.Second)
	for k := 0; k < 2; k++ {
		if k == 0 {
			select {
			case registerChan <- "wo":
				fmt.Printf("Schedule: Write channel ok!\n")
			case <-ticker1.C:
				fmt.Printf("Schedule: Write channel timeout!\n")
			}
		} else {
			select {
			case tmp := <-registerChan:
				fmt.Printf("Schedule: Read channel ok!%s\n", tmp)
			case <-ticker1.C:
				fmt.Printf("Schedule: Read channel timeout!\n")
			}
		}

	}
	ticker1.Stop()
	fmt.Printf("Schedule: %v done\n", phase)
}
