package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.

	files           []string
	nMap            int
	nReduce         int
	assignId        int
	tasks           map[int]*Task
	reduceXa        [][]int
	mapTaskCount    int
	reduceTaskCount int
	taskChan        chan *Task

	finished bool
	mu       sync.Mutex
}

type Status int

const (
	UnFinished Status = iota
	Finished
	Abort
)

type Task struct {
	taskId   int
	reduceId int
	fileName string
	status   Status
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) Schedule(args *TaskArgs, reply *TaskReply) error {

	if args.TaskId != -1 {
		// fmt.Println("args.TaskId:", args.TaskId)
		c.mu.Lock()
		task := c.tasks[args.TaskId]
		if task.status == UnFinished {
			task.status = Finished
			if task.reduceId == -1 {
				for i := range c.reduceXa {
					c.reduceXa[i] = append(c.reduceXa[i], args.TaskId)
				}

				c.mapTaskCount++
				// fmt.Println("c.mapTaskCount:", c.mapTaskCount)
				if c.mapTaskCount == c.nMap {
					for i := 0; i < c.nReduce; i++ {
						c.taskChan <- &Task{
							taskId:   c.assignId,
							reduceId: i,
						}
						c.assignId++
					}
				}

			} else {
				c.reduceTaskCount++
				if c.reduceTaskCount == c.nReduce {
					close(c.taskChan)
					c.finished = true
				}
			}
		}
		c.mu.Unlock()
	}

	task, ok := <-c.taskChan
	if !ok {
		reply.TaskId = -1
	} else {
		reply.TaskId = task.taskId
		reply.ReduceId = task.reduceId

		c.mu.Lock()
		if task.reduceId == -1 {
			reply.NReduce = c.nReduce
			reply.FileName = task.fileName
		} else {
			reply.Xa = c.reduceXa[task.reduceId]
		}
		c.tasks[task.taskId] = task
		c.mu.Unlock()

		go func(task Task) {
			time.Sleep(10 * time.Second)
			c.mu.Lock()
			if c.tasks[task.taskId].status == Finished {
				c.mu.Unlock()
				return
			}
			fmt.Println("===== abort:", task.taskId)
			c.tasks[task.taskId] = &Task{
				status: Abort,
			}
			task.taskId = c.assignId
			c.assignId++
			c.taskChan <- &task
			c.mu.Unlock()
		}(*task)
	}

	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	c.mu.Lock()
	ret = c.finished
	c.mu.Unlock()

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:           files,
		nMap:            len(files),
		nReduce:         nReduce,
		assignId:        0,
		tasks:           map[int]*Task{},
		reduceXa:        make([][]int, nReduce),
		mapTaskCount:    0,
		reduceTaskCount: 0,
		taskChan:        make(chan *Task, len(files)+nReduce),
		finished:        false,
	}
	for i := 0; i < c.nMap; i++ {
		c.reduceXa[i] = make([]int, 0)
		c.taskChan <- &Task{
			taskId:   c.assignId,
			reduceId: -1,
			fileName: files[i],
		}
		c.assignId++
	}

	// Your code here.

	c.server()
	return &c
}
