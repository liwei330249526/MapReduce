package mr

import (
	"fmt"
	"log"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"


type MRTask struct {
	typet int   // 任务类型 TaskMap  TaskReduce 两种任务
	index int	// 在任务列表中的索引
	name string	// 任务名字,
	startTime time.Time	// 开启时间
	workerId int  // 分配给哪个 worker 了
	reduceId int
	done int		// 0 , 未完成; 1,已完成
}

type Master struct {
	// Your definitions here.
	mpTask []MRTask  	// map 任务列表
	rdTask []MRTask	// reduce 任务列表
	taskBuffer chan MRTask		// 过来一个worker, 则从这里获取一个task
	nReduce int
	nMap int
	workerId int				// 分配给worker的 id, 唯一标识一个worker
	done bool
}

// Your code here -- RPC handlers for the worker to call.

// Example
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
// 客户端拨号, 拨到这里
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	fmt.Printf("master receve value=%d\n", args.X)
	return nil
}
// 是否所有 map 任务都完成了
func (m *Master)IsAllMpTaskDone() bool {
	for i := 0; i < len(m.mpTask); i++ {
		if m.mpTask[i].done == 0 {
			return false
		}
	}
	return true
}

// 是否所有 red 任务都完成了
func (m *Master)IsAllRdTaskDone() bool {
	if len(m.rdTask) == 0 {
		return false				// reduce 还未执行
	}
	for i := 0; i < len(m.rdTask); i++ {
		if m.rdTask[i].done == 0 {
			return false
		}
	}
	return true
}

func (m *Master) GetTask(args *TaskArgs, reply *TaskReply) error {		// worker 远程调用获取task
	if m.IsAllMpTaskDone() && len(m.rdTask) == 0 {					// 如果所有 map 任务都结束了, 则创建 reduce 任务, 并加入通
		fmt.Println("所有 map 任务都结束了, 创建 reduce 任务")
		m.rdTask = make([]MRTask, m.nReduce) 	// 创建rd任务
		for i := 0; i < m.nReduce; i++ {
			m.rdTask[i] = MRTask{typet: TaskReduce, index:i, reduceId:i}
		}
		fmt.Println("test1")
		for i := 0; i < m.nReduce; i++ {
			m.taskBuffer <- m.rdTask[i]
			fmt.Println("m.taskBuffer <- m.rdTask[i] = ", i)
		}
		fmt.Println("test2")
	}

	if len(m.rdTask) != 0 && m.IsAllRdTaskDone() {	// 如果所有 red 任务页结束了, 则告诉 worker, 任务结束了, 你可以退出了
		reply.Cmd = TaskEnd
		fmt.Println("所有 red 任务页结束了, 则告诉 worker, 任务结束了, 你可以退出了")
		m.done = true
		return nil
	}
	//fmt.Println("----m=", m)
	// 从缓冲通道获取一个任务, 如果缓冲区为空, 则阻塞等待
	task := <- m.taskBuffer			// 从通道中获取一个任务, 如果最后没有任务了, 则阻塞, 等待一个任务过来, 可能有个未完成的任务, 后续会过来重试.

	reply.Cmd = task.typet			// 返回值
	reply.Index = task.index
	reply.Name = task.name
	reply.ReduceId = task.reduceId
	reply.NReduce = m.nReduce
	reply.NMap = m.nMap
	reply.WorkerId = m.workerId


	if task.typet == TaskMap {
		m.mpTask[task.index].startTime = time.Now()
		m.mpTask[task.index].workerId = m.workerId
	} else if task.typet == TaskReduce {
		m.rdTask[task.index].startTime = time.Now()
		m.rdTask[task.index].workerId =  m.workerId
	}
	m.workerId++
	fmt.Printf("分配一个任务, 名字=%s, 类型=%d, 索引=%d\n", task.name, task.typet, task.index)

	return nil
}

// 当 map 任务没有了, 要转换为 reduce 任务
// 什么时候可以判断map任务执行完成了
func (m *Master) TaskDone(args *TaskArgs, reply *TaskReply) error {
	var task* MRTask
	fmt.Printf("任务结束, 任务名字=%s, 任务类型=%d, 任务索引=%d\n", args.Name, args.Cmd, args.Index)
	fmt.Println("任务结束, task=", args)
	if args.Cmd == TaskMap {
		if args.Index >= len(m.mpTask) {
			// 错误处理
			reply.Cmd = -1
			fmt.Println("TaskDone fail1")
		}
		reply.Cmd = TaskMap
		task =&(m.mpTask[args.Index])
		fmt.Println("task= ", task)

	} else if args.Cmd == TaskReduce {
		if args.Index >= len(m.rdTask) {
			// 错误处理
			reply.Cmd = -1
			fmt.Println("TaskDone fail2")
		}
		reply.Cmd = TaskReduce
		task =&(m.rdTask[args.Index])
	}

	if task.workerId != args.WorkerId {		// 此任务应该分配给 args 请求的任务
		reply.Cmd = -1
		fmt.Printf("TaskDone fail3, 我的task workerid is %d, 收到的 workerid is %d\n", task.workerId, args.WorkerId)
		// 错误处理
	}
	// 将原任务标志已完成
	task.done = 1
	fmt.Println("TaskDone success!")
	return nil
}

// 周期处理任务, 如果任务没有完成, 且超时了, 则加入通道, 重新分配
func (m *Master)HandleDoneTask() {
	for {
		for i := 0; i < len(m.mpTask); i++ {
			if m.mpTask[i].workerId != 0 && m.mpTask[i].done == 0 && time.Now().Sub(m.mpTask[i].startTime) >= time.Duration(time.Second * 2){
				fmt.Printf("1 重新准备任务 index=%d", m.mpTask[i].index)
				m.taskBuffer <- m.mpTask[i]									// 任务这段时间没有执行完成, 则重新分配到 buffer
			}
		}
		for i := 0; i < len(m.rdTask); i++ {
			if m.rdTask[i].workerId != 0 && m.rdTask[i].done == 0 && time.Now().Sub(m.rdTask[i].startTime) >= time.Duration(time.Second * 2) {
				fmt.Printf("2 重新准备任务 index=%d", m.rdTask[i].index)
				m.taskBuffer <- m.rdTask[i]									// 任务重新分配到 buffer
			}
		}
		time.Sleep(time.Second * 5)
	}
}

//
// start a thread that listens for RPCs from worker.go   监听一个rpc 来自worker的
//
func (m *Master) server() {
	fmt.Println("server1")
	rpc.Register(m)							// 注册方法类
	rpc.HandleHTTP()						// http, 一些固定 rpc 地址
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	fmt.Println("server2")
	os.Remove(sockname)
	fmt.Println("server3")
	l, e := net.Listen("unix", sockname)		// 监听这个 sockname
	if e != nil {
		log.Fatal("listen error:", e)
	}
	fmt.Println("server4")
	go http.Serve(l, nil)						// 服务这个 listen
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//main/mrmaster.go周期性地调用Done（）以确定整个作业是否已完成。
func (m *Master) Done() bool {

	// Your code here.
	// 判断没有任务了, 且所有任务都完成了, 则返回
	return m.done
}

// MakeMaster
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
// 传入10个文件名
//  每个文件作为一个任务, 分配下去.; map 任务-- 文件名, reduce 任务-文件名
//  每个 workder 请求过来, 我返回他一个任务, map -- 文件名或 reduce - 文件名.; map 任务处理完
/*
	master   10 个文件, 10个map 任务, 过来一个workder 分配一个 map 任务;   map任务信息: 文件名,输出nReduce个文件, 输出文件名:mr-m-1..2..3..n
									直到map任务没有了, 则分配给workder reduce 任务; reduce 任务信息, 文件名, mr-***n 的所有文件; 输出文件名:mr-out-n
		例如: 文件 pg-being_ernest.txt  --- worker Map 1 -->  临时文件 mr-1-1, mr-1-2,mr-1-3
			 文件 pg-frankenstein.txt  --- worker Map 2 -->  临时文件 mr-2-1, mr-2-2,mr-2-3
			 文件 pg-grimm.txt         --- worker Map 3 -->  临时文件 mr-3-1, mr-3-2,mr-3-3

			mr-1-1  mr-2-1  mr-3-1	   --- worker Rd 4 -->  mr-out-1
			mr-1-2  mr-2-2  mr-3-2     --- worker Rd 5 -->  mr-out-2
			mr-1-3  mr-2-3  mr-3-3	   --- worker Rd 6 -->  mr-out-3


*/
func MakeMaster(files []string, nReduce int) *Master {		// nReduce 个 reduce
	m := Master{}
	fmt.Println("MakeMaster","files=", files, " nReduce=", nReduce)
	taskNum := len(files)
	m.mpTask = make([]MRTask, taskNum)					// 创建mp任务
	m.workerId = 0
	for i := 0; i < len(m.mpTask); i++ {
		m.mpTask[i] = MRTask{typet:TaskMap, index:i, name:files[i]}
	}

	chLen := len(files)
	if chLen < nReduce {
		chLen = nReduce
	}

	m.taskBuffer = make(chan MRTask, chLen)				// 向通道发送任务, 后续的 worker 从这里获取任务
	for i := 0; i < len(files); i++ {
		m.taskBuffer <- m.mpTask[i]
	}
	m.nReduce = nReduce
	m.nMap = len(files)
	// 开启协程执行
	go m.HandleDoneTask()

	//MakeMaster files= [pg-being_ernest.txt pg-dorian_gray.txt pg-frankenstein.txt pg-grimm.txt pg-huckleberry_finn.txt pg-metamorphosis.txt pg-sherlock_holmes.txt pg-tom_sawyer.txt]  nReduce= 10
	// Your code here.
	// 这里有10个文件, 应该是需要我们去操作的. 建立任务; 如果来一个worker ,则给worker 分配一个任务去做

	m.server()
	return &m
}
