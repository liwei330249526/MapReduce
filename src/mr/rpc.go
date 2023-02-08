package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// 任务命令, worker 请求一个任务, master 说, 你去map吧, 你去reduce 吧, 你去等待下, 任务结束了, 你走吧.
const (
	TaskMap = 1
	TaskReduce = 2
	TaskWait = 3
	TaskEnd = 4
)

type TaskArgs struct {
	Cmd int
	Index int  	// 已经完成的任务在任务列表的位置, 唯一标识一个任务
	Name string
	WorkerId int
}

type TaskReply struct {
	Cmd int
	Index int		// 在原来的任务列表中的位置, 位置索引
	Name string
	ReduceId int
	NReduce int
	NMap int
	WorkerId int
}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
//在/var/tmp中为 主机创建一个 唯一的ish UNIX域套接字名称。
//无法使用当前目录，因为Athena AFS不支持UNIX域套接字。
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
