package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"


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
// 选择一个 reduce 任务号 为每个keyvalue
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

/*

	例如: 文件 pg-being_ernest.txt  --- worker Map 1 -->  临时文件 mr-1-1, mr-1-2, mr-1-3
		 文件 pg-frankenstein.txt  --- worker Map 2 -->  临时文件 mr-2-1, mr-2-2, mr-2-3
		 文件 pg-grimm.txt         --- worker Map 3 -->  临时文件 mr-3-1, mr-3-2, mr-3-3

		mr-1-1  mr-2-1  mr-3-1	   --- worker Rd 4 -->  mr-out-1
		mr-1-2  mr-2-2  mr-3-2     --- worker Rd 5 -->  mr-out-2
		mr-1-3  mr-2-3  mr-3-3	   --- worker Rd 6 -->  mr-out-3

*/

// for sorting by key.
type ByKey []KeyValue				// 新类型, Len, Less, Swap
// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

var mapWorkerIds []int

func ProcessMap(mapf func(string, string) []KeyValue, task TaskReply) {
	// 读取文件
	fmt.Println("worker:开启map 任务=", task.Name)
	file, err := os.Open(task.Name)
	if err != nil {
		log.Fatalf("connot open file %v\n", task.Name)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("connot read file %v\n", task.Name)
	}
	// map 处理
	kva := mapf(task.Name, string(content))		// kv数组
	//sort.Sort(ByKey(kva))						// 排序

	kvaM := make(map[int][]KeyValue)			// 用map 分文件

	for i := 0; i < len(kva); i++ {
		id := ihash(kva[i].Key) % task.NReduce
		kvaM[id] = append(kvaM[id], kva[i])
	}
	for i := 0; i < task.NReduce; i++ {				// n 个reduce 共生成 n 个文件
		outFileName := fmt.Sprintf("mr-%d-%d", task.WorkerId, i)
		mapWorkerIds = append(mapWorkerIds, task.WorkerId)
		outFile, err := os.Create(outFileName)
		if err != nil {
			log.Fatalf("connot Create file %v\n", outFileName)
		}

		for j := 0; j < len(kvaM[i]); j++ {			// map[i] 代表写入一个文件中的内容
			fmt.Fprintf(outFile, "%v\t%v\n", kvaM[i][j].Key, kvaM[i][j].Value)
		}
		outFile.Close()
	}
}

func ProcessReduce(reducef func(string, []string) string, task TaskReply) {
	// 读取文件内容
	// 读取文件
	kva := []KeyValue{}
	for j := 0; j < task.NMap; j++ {
		inFileName := fmt.Sprintf("mr-%d-%d", j,task.ReduceId)			//workid-nreduce;  多少个 map 任务
		file, err := os.Open(inFileName)
		if err != nil {
			log.Fatalf("connot open file %v\n", task.Name)
		}

		content, err := ioutil.ReadAll(file)
		fmt.Printf("读取文件名字:%s, 内容大小:%d\n", inFileName, len(content))
		if err != nil {
			log.Fatalf("connot read file %v\n", task.Name)
		}

		// reduce 操作

		lines := strings.Split(string(content), "\n")
		for i := 0; i < len(lines); i++ {
			line := strings.Split(lines[i], "\t")
			if len(line) != 2 {
				continue
			}
			kv := KeyValue{line[0], line[1]}
			kva = append(kva, kv)
		}
		//return kva				// 返回切片
	}

	sort.Sort(ByKey(kva))
	fmt.Println("排序完成, len(kva)=", len(kva))


	// 写入文件
	outFileName := fmt.Sprintf("mr-out-%d",task.ReduceId)
	fmt.Println("文件名字=", outFileName)
	outFile, err := os.Create(outFileName)
	if err != nil {
		log.Fatalf("connot Create file %v\n", outFileName)
	}

	for k := 0; k < len(kva); k++ {			// kva 排序后的, k:v
		values := make([]string, 0)
		values = append(values, kva[k].Value)
		m := k+1
		key := kva[k].Key

		for ; m < len(kva) && kva[k] == kva[m]; m++{	// key相等的value, 都加入values数组, 作为参数 reducef
			values = append(values, kva[k].Value)
			k++
		}

		out := reducef(kva[k].Key, values)			// 111 222 333 , 对一组kv reduce处理后将结果写入文件
		
		fmt.Fprintf(outFile, "%v %v\n", key, out)
	}
}

// Worker
// main/mrworker.go calls this function.
// 这里获取两个函数 mapf, reducef
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.
	// 应该要对一些内容左map, reduce 操作, 然后写入存储, 返回给server
	// uncomment to send the Example RPC to the master. 取消注释以将示例RPC发送到主机。
	for {
		task := CallGetTask()
		fmt.Printf("获得任务, task类型%d, task index=%d, task workid=%d\n", task.Cmd, task.Index, task.WorkerId)
		if task.Cmd  == TaskMap {		// 执行 map, task 中包含map操作的所有信息, 即执行哪个文件, 输出什么文件
			ProcessMap(mapf, task)
			fmt.Printf("1任务结束, 名字=%s, 类型=%d, 索引=%d\n", task.Name, task.Cmd, task.Index)
			CallTaskDone(task)
		} else if task.Cmd == TaskReduce {	// 执行reduce, task 包含所有reduce信息, 即执行哪些文件, 输出什么文件
			fmt.Printf("2任务结束, 名字=%s, 类型=%d, 索引=%d\n", task.Name, task.Cmd, task.Index)
			ProcessReduce(reducef, task)
			fmt.Println("一个任务reduce结束")
			CallTaskDone(task)
		} else if task.Cmd == TaskEnd {		// 对面执行结束了, 我可以退出了
			return
		} else {							// 否则, 过一段时间再去申请
			time.Sleep(10 *time.Second)
		}
	}
	return
}

// CallTaskDone 告诉 master, 我执行完了
func CallTaskDone(task TaskReply) TaskReply {
	args := TaskArgs{}
	args.Cmd = task.Cmd
	args.Name = task.Name
	args.Index = task.Index
	args.WorkerId = task.WorkerId
	reply := TaskReply{}

	// 获取了文件名字
	call("Master.TaskDone", &args, &reply)
	fmt.Println("reply", reply)

	return reply
}

func CallGetTask() TaskReply {
	args := TaskArgs{}
	reply := TaskReply{}

	// 获取了文件名字
	call("Master.GetTask", &args, &reply)
	fmt.Println("reply", reply)

	return reply
}


// CallExample
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
// 向master发送 rpc 消息
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.   发送rpc 向master
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.   发送rpc 到master, 等待回复
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()									// DialHTTP连接到指定网络地址的HTTP RPC服务器，侦听默认 HTTP RPC 路径。
	c, err := rpc.DialHTTP("unix", sockname)			// 拨号这个 sock, 获得 C
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)							// c 可以打电话, call 这个 rpcname
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
