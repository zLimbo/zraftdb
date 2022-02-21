package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

	taskId := -1

	for {
		reply, ok := CallSchedule(taskId)
		// fmt.Println("reply:", reply)
		if !ok || reply.TaskId == -1 {
			break
		}

		if reply.ReduceId == -1 {
			runMap(mapf, reply.TaskId, reply.NReduce, reply.FileName)
		} else {
			runReduce(reducef, reply.ReduceId, reply.Xa)
		}
		taskId = reply.TaskId
	}
}

func runMap(mapf func(string, string) []KeyValue, taskId int, nReduce int, fileName string) {
	// start := time.Now()
	file, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("cannot open %v", fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", fileName)
	}
	file.Close()

	intermediates := make([][]KeyValue, nReduce)
	for i := 0; i < nReduce; i++ {
		intermediates[i] = []KeyValue{}
	}
	kva := mapf(fileName, string(content))

	for _, kv := range kva {
		idx := ihash(kv.Key) % nReduce
		intermediates[idx] = append(intermediates[idx], kv)
	}

	for idx, intermediate := range intermediates {
		oname := fmt.Sprintf("mr-%d-%d", taskId, idx)
		ofile, _ := ioutil.TempFile("", oname+".*.tmp")
		defer ofile.Close()

		enc := json.NewEncoder(ofile)
		for _, kv := range intermediate {
			err := enc.Encode(&kv)
			if err != nil {
				os.Remove(ofile.Name())
				log.Fatalf("write %v in %v fial.", kv, ofile)
			}
		}
		// todo 超时
		// spend := time.Since(start)
		os.Rename(ofile.Name(), oname)
	}
}

func runReduce(reducef func(string, []string) string, id int, xa []int) {
	// start := time.Now()
	intermediates := make(map[string][]string)
	for _, x := range xa {
		iname := fmt.Sprintf("mr-%d-%d", x, id)
		file, err := os.Open(iname)
		if err != nil {
			log.Fatalf("cannot open %v", file)
		}
		defer file.Close()
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if intermediates[kv.Key] == nil {
				intermediates[kv.Key] = []string{}
			}
			intermediates[kv.Key] = append(intermediates[kv.Key], kv.Value)
		}
	}
	oname := fmt.Sprintf("mr-out-%d", id)
	ofile, err := ioutil.TempFile("", oname+".*.tmp")
	if err != nil {
		log.Fatalf("cannot create tempfile of %v", oname)
	}
	for key, values := range intermediates {
		output := reducef(key, values)
		fmt.Fprintf(ofile, "%v %v\n", key, output)
	}
	os.Rename(ofile.Name(), oname)
}

func CallSchedule(taskId int) (*TaskReply, bool) {
	args := TaskArgs{TaskId: taskId}
	reply := TaskReply{}

	ok := call("Coordinator.Schedule", &args, &reply)

	return &reply, ok
}

//
// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
