package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"time"
)

var mut sync.Mutex

type PhaseIn int

const (
	MapPhase PhaseIn = iota
	ReducePhase
	AllDone
)

type TaskStatu int

const (
	Waiting TaskStatu = iota
	Working
	Done
)

type TaskInfo struct {
	taskadr   *Task
	status    TaskStatu
	starttime time.Time
}

type Coordinator struct {
	// Your definitions here.
	ReducerNum        int        // 传入的参数决定需要多少个reducer
	TaskId            int        // 用于生成task的特殊id
	DistPhase         PhaseIn    // 目前整个框架应该处于什么任务阶段
	TaskChannelReduce chan *Task // 使用chan保证并发安全
	TaskChannelMap    chan *Task // 使用chan保证并发安全
	// MapFinshedNum     int        // number of MapFinshed so bad using map
	TaskStatuMap    map[int]*TaskInfo
	TaskStatuReduce map[int]*TaskInfo
	files           []string // 传入的文件数组
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {

	// Your code here.
	if c.DistPhase == AllDone {
		return true
	} else {
		return false
	}

}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:             files,
		ReducerNum:        nReduce,
		DistPhase:         MapPhase,
		TaskChannelMap:    make(chan *Task, len(files)),
		TaskChannelReduce: make(chan *Task, nReduce),
		TaskStatuMap:      make(map[int]*TaskInfo, len(files)),
		TaskStatuReduce:   make(map[int]*TaskInfo, nReduce),
	}
	c.MapphaseInit(files)
	// Your code here.

	c.server()
	go c.CrashRecover()
	return &c
}

func (c *Coordinator) MapphaseInit(files []string) {
	for i, filename := range files {
		filenames := []string{
			filename,
		}
		task := Task{
			TaskType:   MapTask,
			TaskId:     i,
			ReducerNum: c.ReducerNum,
			Filenames:  filenames,
		}
		c.TaskChannelMap <- &task
		taskinfo := TaskInfo{
			status:  Waiting,
			taskadr: &task,
		}
		c.TaskStatuMap[i] = &taskinfo
	}
}

func (c *Coordinator) PollTask(args *TaskArgs, reply *Task) error {
	//chose phase
	mut.Lock()
	defer mut.Unlock()
	switch c.DistPhase {
	case MapPhase:
		{
			if len(c.TaskChannelMap) > 0 {
				*reply = *<-c.TaskChannelMap
				c.TaskStatuMap[reply.TaskId].status = Working
				c.TaskStatuMap[reply.TaskId].starttime = time.Now()
			} else {
				reply.TaskType = WaittingTask
				c.CheckAllDoneTask()
			}
		}
	case ReducePhase:
		{
			if len(c.TaskChannelReduce) > 0 {
				*reply = *<-c.TaskChannelReduce
				c.TaskStatuReduce[reply.TaskId].status = Working
				c.TaskStatuReduce[reply.TaskId].starttime = time.Now()
			} else {
				reply.TaskType = WaittingTask
				c.CheckAllDoneTask()
			}
		}
	case AllDone:
		{
			reply.TaskType = ExitTask
		}
	}

	return nil
}

func (c *Coordinator) MarkFinished(args *Task, reply *Task) error {
	mut.Lock()
	defer mut.Unlock()
	switch args.TaskType {
	case MapTask:
		taksinfo := c.TaskStatuMap[args.TaskId]

		if taksinfo.status == Working {
			taksinfo.status = Done
		}

	case ReduceTask:
		taksinfo := c.TaskStatuReduce[args.TaskId]

		if taksinfo.status == Working {
			taksinfo.status = Done
		}
	}
	return nil
}

func (c *Coordinator) ReduceTaskMake() {
	for i := 0; i < c.ReducerNum; i++ {
		var filenames []string
		for j := 0; j < len(c.files); j++ {
			oname := "mr-tmp-" + strconv.Itoa(j) + "-" + strconv.Itoa(i)
			filenames = append(filenames, oname)
		}
		task := Task{
			TaskType:   ReduceTask,
			TaskId:     i,
			ReducerNum: c.ReducerNum,
			Filenames:  filenames,
		}
		c.TaskChannelReduce <- &task

		taskinfo := TaskInfo{
			status:  Waiting,
			taskadr: &task,
		}
		c.TaskStatuReduce[i] = &taskinfo
	}
}

func (c *Coordinator) CheckAllDoneTask() {
	var mapCompeletNum int
	var reduceCompeletNum int
	switch c.DistPhase {
	case MapPhase:
		for _, taskinfo := range c.TaskStatuMap {
			if taskinfo.status == Done {
				mapCompeletNum++
			}
		}
		if mapCompeletNum == len(c.files) {
			c.ReduceTaskMake()
			c.DistPhase = ReducePhase
			// log.Println("create reducetask")
		}
	case ReducePhase:
		for _, taskinfo := range c.TaskStatuReduce {
			if taskinfo.status == Done {
				reduceCompeletNum++
			}
		}
		if reduceCompeletNum == c.ReducerNum {
			c.DistPhase = AllDone
		}
	}
}

func (c *Coordinator) CrashRecover() {
	for {
		time.Sleep(time.Second * 2)
		mut.Lock()
		switch c.DistPhase {
		case MapPhase:
			{
				for _, taskinfo := range c.TaskStatuMap {
					if taskinfo.status == Working && time.Since(taskinfo.starttime) > 9*time.Second {
						// log.Println(taskinfo.taskadr.TaskType)
						// log.Println(taskinfo.taskadr.TaskId)
						c.TaskChannelMap <- taskinfo.taskadr
						taskinfo.status = Waiting
					}
				}
			}
		case ReducePhase:
			{
				for _, taskinfo := range c.TaskStatuReduce {
					if taskinfo.status == Working && time.Since(taskinfo.starttime) > 9*time.Second {
						// log.Println(taskinfo.taskadr.TaskType)
						// log.Println(taskinfo.taskadr.TaskId)
						c.TaskChannelReduce <- taskinfo.taskadr
						taskinfo.status = Waiting
					}
				}
			}
		case AllDone:
			{
				mut.Unlock()
				return
			}
		}
		mut.Unlock()
	}
}
