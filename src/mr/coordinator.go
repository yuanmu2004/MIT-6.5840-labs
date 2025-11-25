package mr

import (
	// "fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

// import "container/heap"


type TaskStamp int64

func (t TaskStamp) Before(u TaskStamp) bool {
	return t < u
}

func CurrentStamp() TaskStamp {
	return TaskStamp(time.Now().UnixNano())
}

func Current2ExpiryStamp() TaskStamp {
	return TaskStamp(time.Now().Add(time.Second * 10).UnixNano())
}

// type StampHeap []TaskStamp


// func (h StampHeap) Len() int {
// 	return len(h)
// }

// func (h StampHeap) Less(i, j int) bool {
// 	return h[i] < h[j]
// }

// func (h StampHeap) Swap(i, j int) {
// 	h[i], h[j] = h[j], h[i]
// }

// func (h *StampHeap) Push(x interface{}) {
// 	*h = append(*h, x.(TaskStamp))
// }

// func (h *StampHeap) Pop() interface{} {
// 	old := *h
// 	n := len(old)
// 	x := old[n-1]
// 	*h = old[0 : n-1]
// 	return x
// }

// // 取出一个超时任务的时间戳
// func (h *StampHeap) Timeout() TaskStamp {
// 	peak := (*h)[0]
// 	if peak.Before(CurrentStamp()) {
// 		return h.Pop().(TaskStamp)
// 	}
// 	return -1
// }



// type Task struct {
	
// }

// type MapTaskQueue struct {
// 	// 分配优先级：超时>未分配
// 	// 未分配任务
// 	// Map任务不关心workers的数量，一个大列表即可
// 	tset []*Task
// 	// 已分配任务，使用小根堆维护超时
// 	theap StampHeap
// 	tmap map[TaskStamp]*Task
// }

// func (q *MapTaskQueue) Register(task *Task) {
// 	q.tset = append(q.tset, task)
// }

// func (q *MapTaskQueue) Pop() *Task {
// 	// 分配优先级：超时>未分配
// 	if t := q.theap.Timeout(); t != -1 {
// 		return q.tmap[t]
// 	}
// 	if n := len(q.tset); n > 0 {
// 		task := q.tset[0]
// 		q.tset = q.tset[1:]
// 		return task
// 	}
// 	return nil
// }

// type ReduceTaskQueue struct {
// 	// Reduce任务的worker数取决于nReduce，开nReduce个桶
// 	tset [][]*Task
// 	// 已分配任务，使用小根堆维护超时
// 	theap StampHeap
// 	tmap map[TaskStamp]*Task
// }

// func (q *ReduceTaskQueue) Register(task *Task) {
// 	// Reduce任务的worker数取决于nReduce，开nReduce个桶
// }


const (
	MapTask = iota
	ReduceTask
	Waiting
	Finish
)

const (
	Idle = iota
	Working
	Done
)


type Task struct {
	TaskType int
	TaskId int
	Filename string
	TaskStamp TaskStamp
	TaskStatus int
}


type RequestMsg struct {
	request *TaskRequestArgs;
	ok chan struct{}
}



type ReplyMsg struct {
	reply *TaskRequestReply;
	ok chan struct{}
}




type Coordinator struct {
	// Your definitions here.
	Tasks []Task
	NMap int
	NReduce int
	requestCh chan *RequestMsg
	replyCh chan *ReplyMsg
	doneCh chan struct{}

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


// 报告任务完成的同时，请求新的任务
func (c *Coordinator) TaskRequest(args *TaskRequestArgs, reply *TaskRequestReply) error {
	requestMsg := RequestMsg{args, make(chan struct{})}
	c.requestCh <- &requestMsg
	<- requestMsg.ok
	// fmt.Printf("Args: %+v\n", *args)

	replyMsg := ReplyMsg{reply, make(chan struct{})}
	c.replyCh <- &replyMsg
	<- replyMsg.ok
	// fmt.Printf("Reply: %+v\n", *reply)

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

func (c *Coordinator) master() {
	// Map阶段
	nMapDone := 0
	Outer:
	for {
		select {
		case reqMsg := <- c.requestCh:
			// 更改任务状态
			// Do something
			if reqMsg.request.TaskId >= 0 &&
			 reqMsg.request.TaskId < c.NMap &&
			 c.Tasks[reqMsg.request.TaskId].TaskStatus == Working {
				c.Tasks[reqMsg.request.TaskId].TaskStatus = Done
				// println("nMapReduce + 1");
				nMapDone += 1
			}
			reqMsg.ok <- struct{}{}			
		case replyMsg := <- c.replyCh:
			// 发送任务
			// Do something
			TaskType := MapTask
			// nMap := len(c.Tasks)
			// 优先超时任务
			for i := 0; i < c.NMap; i++ {
				if c.Tasks[i].TaskStatus == Done {
					continue
				}
				curStamp := CurrentStamp()
				if c.Tasks[i].TaskStatus == Idle || (c.Tasks[i].TaskStamp.Before(curStamp)) {
					c.Tasks[i].TaskStatus = Working
					c.Tasks[i].TaskStamp = Current2ExpiryStamp()
					// c.Tasks[i].TaskType = TaskType
					// c.Tasks[i].TaskId = i
					// c.Tasks[i].Filename = c.Tasks[i].Filename
					replyMsg.reply.TaskType = TaskType
					replyMsg.reply.TaskId = i
					replyMsg.reply.Filename = c.Tasks[i].Filename
					// replyMsg.reply.NMap = c.NMap
					replyMsg.reply.NReduce = c.NReduce
					replyMsg.reply.TaskStamp = c.Tasks[i].TaskStamp
					replyMsg.ok <- struct{}{}
					continue Outer
				}
			}
			// 没有可分配任务，所有worker待命、
			replyMsg.reply.TaskId = -1
			replyMsg.reply.TaskStamp = -1
			replyMsg.reply.TaskType = Waiting
			replyMsg.ok <- struct{}{}
		}
		if nMapDone == c.NMap {
			break
		}
	}
 
	// Reduce阶段
	c.Tasks = make([]Task, c.NReduce)
	nReduceDone := 0
	// println(c.NReduce)
	for i := 0; i < c.NReduce; i++ {
		c.Tasks[i] = Task{ReduceTask, i, "", -1, Idle}
	}
	Outer2:
	for {
		select {
		case reqMsg := <- c.requestCh:
			// 更改任务状态
			// Do something
			if reqMsg.request.TaskId >= 0 &&
			 reqMsg.request.TaskId < c.NReduce &&
			 c.Tasks[reqMsg.request.TaskId].TaskStatus == Working {
				c.Tasks[reqMsg.request.TaskId].TaskStatus = Done
				nReduceDone += 1
			}
			reqMsg.ok <- struct{}{}			
		case replyMsg := <- c.replyCh:
			// 发送任务
			// Do something
			TaskType := ReduceTask
			// nReduce := len(c.Tasks)
			// 优先超时任务
			for i := 0; i < c.NReduce; i++ {
				if c.Tasks[i].TaskStatus == Done {
					continue
				}
				curStamp := CurrentStamp()
				if c.Tasks[i].TaskStatus == Idle || (c.Tasks[i].TaskStamp.Before(curStamp)) {
					c.Tasks[i].TaskStatus = Working
					c.Tasks[i].TaskStamp = Current2ExpiryStamp()
					// c.Tasks[i].TaskType = TaskType
					// c.Tasks[i].TaskId = i
					// c.Tasks[i].Filename = c.Tasks[i].Filename
					replyMsg.reply.TaskType = TaskType
					replyMsg.reply.TaskId = i
					// replyMsg.reply.Filename = c.Tasks[i].Filename
					replyMsg.reply.NMap = c.NMap
					// replyMsg.reply.NReduce = c.NReduce
					replyMsg.reply.TaskStamp = c.Tasks[i].TaskStamp
					replyMsg.ok <- struct{}{}
					continue Outer2
				}
			}
			// 没有可分配任务，所有worker待命、
			replyMsg.reply.TaskId = -1
			replyMsg.reply.TaskStamp = -1
			replyMsg.reply.TaskType = Waiting
			replyMsg.ok <- struct{}{}
		}
		if nReduceDone == c.NReduce {
			break
		}

	}
	c.doneCh <- struct{}{}
	for {
		select {
		case reqMsg := <- c.requestCh:
			// 无任务可做
			reqMsg.ok <- struct{}{}			
		case replyMsg := <- c.replyCh:
			// 发送Finish
			replyMsg.reply.TaskType = Finish
			replyMsg.ok <- struct{}{}
		}
	}
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.
	select {
	case <- c.doneCh:
		ret = true
	default:
		ret = false
	}
	

	return ret
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	nMap := len(files)
	c := Coordinator{
		make([]Task, 0),
		nMap, 
		nReduce,
		make(chan *RequestMsg),
		make(chan *ReplyMsg),
		make(chan struct{}),
	}
	for i := range nMap {
		c.Tasks = append(c.Tasks, Task{MapTask, i, files[i], -1, Idle})
	}
	go c.master()
	c.server()
	return &c
}
