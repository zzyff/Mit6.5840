package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"strconv"
	"time"
)

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// Task worker向coordinator获取task的结构体
type Task struct {
	TaskType   TaskType // 任务类型判断到底是map还是reduce
	TaskId     int      // 任务的id
	ReducerNum int      // 传入的reducer的数量，用于hash
	Filenames  []string // 输入文件
}

type TaskArgs struct{}

// TaskType 对于下方枚举任务的父类型
type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	WaittingTask // Waitting任务代表此时为任务都分发完了，但是任务还没完成，阶段未改变
	ExitTask     // exit
)

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	going := true
	for going {
		task := GetTask()
		switch task.TaskType {
		case MapTask:
			{
				Mapopearator(mapf, task.TaskId, task.Filenames[0], task.ReducerNum)
				callDone(&task)
			}
		case WaittingTask:
			{
				time.Sleep(time.Second)
			}
		case ReduceTask:
			{
				ReduceOperator(reducef, &task)
				callDone(&task)
			}
		case ExitTask:
			going = false
		}

	}

}

func GetTask() Task {

	args := TaskArgs{}
	reply := Task{}
	ok := call("Coordinator.PollTask", &args, &reply)

	if ok {
		// fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}

func callDone(task *Task) Task {

	// args := Task{}
	reply := Task{}
	ok := call("Coordinator.MarkFinished", &task, &reply)

	if ok {
		// fmt.Println(reply)
	} else {
		fmt.Printf("call failed!\n")
	}
	return reply

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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

// this is worker map opearator,input is filename and map function
// no return
func Mapopearator(mapf func(string, string) []KeyValue, taskID int, filename string, reducenum int) {

	intermediate := []KeyValue{}
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	defer file.Close()
	intermediate = mapf(filename, string(content))
	//
	// a big difference from real MapReduce is that all the
	// intermediate data is in one place, intermediate[],
	// rather than being partitioned into NxM buckets.
	//
	//slice intermediate in reducenum slice
	HashedKV := make([][]KeyValue, reducenum)
	for _, kv := range intermediate {
		HashedKV[ihash(kv.Key)%reducenum] = append(HashedKV[ihash(kv.Key)%reducenum], kv)
	}

	//create middle file to save kv value
	for i := 0; i < reducenum; i++ {

		oname := "mr-tmp-" + strconv.Itoa(taskID) + "-" + strconv.Itoa(i)
		//create tempfile
		tempFile, err := os.CreateTemp("/tmp", oname)
		if err != nil {
			fmt.Println("Failed to create temporary file:", err)
			return
		}
		defer os.Remove(tempFile.Name())
		enc := json.NewEncoder(tempFile)
		//write to tempfile
		for _, kv := range HashedKV[i] {
			enc.Encode(kv)
		}
		err = os.Rename(tempFile.Name(), oname)
		if err != nil {
			fmt.Println("Failed to rename temporary file:", err)
			return
		}
	}
}

// This is worker reduce function
//
// input is reduce function,Task
func ReduceOperator(reducef func(string, []string) string, response *Task) {

	reduceID := response.TaskId
	intermediate := shuffle(response.Filenames)
	oname := "mr-tmp-" + strconv.Itoa(reduceID)
	//create tempfile
	tempFile, err := os.CreateTemp("/tmp", oname)
	if err != nil {
		log.Println("Failed to create tempory file", err)
	}
	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(tempFile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}

	tempFile.Close()
	fn := fmt.Sprintf("mr-out-%d", reduceID)
	os.Rename(tempFile.Name(), fn)

}

// This shuffle function
//
// Input is filenames
func shuffle(filnames []string) []KeyValue {
	var kva []KeyValue
	for _, filename := range filnames {
		file, err := os.Open(filename)
		if err != nil {
			log.Fatalf("cannot open %v", filename)
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}
	sort.Sort(ByKey(kva))
	return kva
}
