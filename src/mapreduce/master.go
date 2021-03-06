package mapreduce

import "container/list"
import "fmt"
//import "time"
// import "reflect"

type WorkerInfo struct {
	address string
	// You can add definitions here.
	available bool
}


// Clean up all workers by sending a Shutdown RPC to each one of them Collect
// the number of jobs each work has performed.
func (mr *MapReduce) KillWorkers() *list.List {
	l := list.New()
	for _, w := range mr.Workers {
		DPrintf("DoWork: shutdown %s\n", w.address)
		args := &ShutdownArgs{}
		var reply ShutdownReply;
		ok := call(w.address, "Worker.Shutdown", args, &reply)
		if ok == false {
			fmt.Printf("DoWork: RPC %s shutdown error\n", w.address)
		} else {
			l.PushBack(reply.Njobs)
		}
	}
	return l
}

func (mr *MapReduce) RunMaster() *list.List {



	mr.mapQue = make([]string, mr.nMap, mr.nMap)

	mr.reduceQue = make([]string, mr.nReduce, mr.nReduce)
	
	nMap := mr.nMap
	nReduce := mr.nReduce
	
	finishedMap := nMap
	finishedReduce := nReduce
	
	doneJob := make(chan int)

	//fmt.Println("aaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", nMap, "\t", nReduce)
	//thread to get all workers and add them to the workersQue until all map and reduce jobs have been done
	go func() {
		for {
			
	  		worker := <-mr.registerChannel
	  		if worker != "" {
	  			mr.workersQue <- worker
	  			fmt.Println("master get workers:", worker)
	  		}
	  		
	  		if (finishedMap == 0) && (finishedReduce == 0) {
	  			break
	  		}
		}
	}()

	
	//find jobs and allocate a worker to do these jobs until
	go func() {
		for nMap > 0 {
		
			i := mr.nMap - nMap
			nMap --
		
		
			go func () {
				for {
					worker := <-mr.workersQue
			  		//fmt.Println("master distribute no.", i, "worker:", worker)
			  		jobArgs := new(DoJobArgs)
					jobArgs.File = mr.file
					jobArgs.Operation = Map
					jobArgs.JobNumber = i
					jobArgs.NumOtherPhase = mr.nReduce
					var reply DoJobReply	
					ok := call(worker, "Worker.DoJob", jobArgs, &reply)
					if ok {
						mr.workersQue <- worker
						doneJob <- 1
						break;
					} else {
						fmt.Println("master woker failed:", worker)
						//break
					}
			  	}
			  	
			}()
		}
	}()
	
	for {
		<- doneJob
		finishedMap --
		if finishedMap == 0 {
			break
		}
	}
	
	go func() {
		for nReduce > 0 {
	
			i := mr.nReduce - nReduce
			nReduce --
	
			go func () {
				for {
					worker := <-mr.workersQue
			  		
			  		jobArgs := new(DoJobArgs)
					jobArgs.File = mr.file
					jobArgs.Operation = Reduce
					jobArgs.JobNumber = i
					jobArgs.NumOtherPhase = mr.nMap
					var reply DoJobReply	
					ok := call(worker, "Worker.DoJob", jobArgs, &reply)
					if ok {
						mr.workersQue <- worker
						doneJob <- 1
						break;
					} else {
						fmt.Println("master woker failed:", worker)
						//break
					}
			  	}
			  	
			}()
		}
	}()

	for {
		<- doneJob
		finishedReduce --
		if finishedReduce == 0 {
			break
		}
	}

	return mr.KillWorkers()
}
