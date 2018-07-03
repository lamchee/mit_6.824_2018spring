package mapreduce

import (
	"fmt"
	"sync"
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
		fmt.Printf("Try to catch a worker, current task index : %d\n", i)
		worker := <-registerChan
		fmt.Printf("Dispatch work, taskNumber : %d, worker : [%s]\n", i, worker)

		var args DoTaskArgs
		args.JobName = jobName
		args.NumOtherPhase = n_other
		args.Phase = phase
		args.TaskNumber = i
		args.File = mapFiles[i]
		go func(w string, tasks int, args DoTaskArgs) {
			defer wg.Done()
			fmt.Printf("Going to process job, worker : [%s], taskNumber : %d\n", worker, args.TaskNumber)
			if call(worker, "Worker.DoTask", args, nil) {
				registerChan <- worker
				fmt.Printf("Job done! worker : [%s], taskNumber : %d\n", worker, args.TaskNumber)
			} else {
				taskChan <- tasks
				fmt.Printf("Job failed! worker : [%s], taskNumber : %d\n", worker, args.TaskNumber)
			}
		}(worker, ntasks, args)
	}
	fmt.Printf("Schedule: %v done\n", phase)
}
