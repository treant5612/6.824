package mr

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"os"
	"syscall"
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
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	period := time.Tick(time.Second)
	for range period {
		task := askForTask()
		if task == nil {
			continue
		}
		if task.AllCompleted {
			return
		}
		handleTask(task, mapf, reducef)
	}

}

func handleTask(task *Task, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	if task.Phase == MapPhase {
		doMap(task, mapf)
	} else {
		doReduce(task, reducef)
	}
	notifyTaskDone(task)
}

func doMap(task *Task, mapF func(string, string) []KeyValue) {
	file := task.FileName
	content, err := ioutil.ReadFile(file)
	if err != nil {
		panic(fmt.Sprintf("read file error:%v", err))
	}
	kvs := mapF(file, string(content))
	reduceTotal := task.OtherTaskNum
	mapNun := task.TaskNum
	files := make([]*os.File, reduceTotal)
	encoders := make([]*json.Encoder, reduceTotal)

	for i := range encoders {
		finalName := intermediateName(mapNun, i)
		fileName := finalName
		f, err := os.Create(fileName)
		if err != nil {
			panic(fmt.Sprintf("create file %v error:%v", fileName, err))
		}
		//defer os.Rename(tempName, finalName)
	
		defer f.Close()
		e := syscall.Flock(int(f.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
		if e != nil {
			return
		}
		defer syscall.Flock(int(f.Fd()), syscall.LOCK_UN)
		files[i] = f
		encoders[i] = json.NewEncoder(f)
	}
	for i := range kvs {
		reduceNum := ihash(kvs[i].Key) % reduceTotal
		encoders[reduceNum].Encode(kvs[i])
	}

}

func doReduce(task *Task, reduceF func(string, []string) string) {
	reduceNum, mapTotal := task.TaskNum, task.OtherTaskNum
	data := make(map[string][]string)

	for i := 0; i < mapTotal; i++ {
		fileName := intermediateName(i, reduceNum)
		file, err := os.Open(fileName)
		if err != nil {
			panic(fmt.Sprintf("error opening file %v:%v", fileName, err))
		}
		decoder := json.NewDecoder(file)
		for {
			kv := new(KeyValue)
			err = decoder.Decode(kv)
			if err != nil {
				break
			}
			data[kv.Key] = append(data[kv.Key], kv.Value)
		}
		file.Close()
	}
	tempName := outName(reduceNum) //tempName()
	//	tempName = outName(reduceNum)
	outFile, err := os.Create(tempName)
	if err != nil {
		panic("create outfile error:" + err.Error())
	}
	defer outFile.Close()
	e := syscall.Flock(int(outFile.Fd()), syscall.LOCK_EX|syscall.LOCK_NB)
	if e != nil {
		return
	}
	defer syscall.Flock(int(outFile.Fd()), syscall.LOCK_UN)
	
	for key := range data {
		value := reduceF(key, data[key])
		outFile.WriteString(fmt.Sprintf("%v %v\n", key, value))
	}
}

//
// example function to show how to make an RPC call to the master.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}
	
	// fill in the argument(s).
	args.X = 99
	
	// declare a reply structure.
	reply := ExampleReply{}
	
	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)
	
	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", "mr-socket")
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}
	return false
}

func askForTask() *Task {
	task := new(Task)
	ok := call("Master.HandOutTask", &struct{}{}, task)
	if ok {
		return task
	}
	return nil
}
func notifyTaskDone(task *Task) {
	_ = call("Master.DoneTask", task, &struct{}{})
}

func intermediateName(x int, y int) string {
	return fmt.Sprintf("mr-%d-%d", x, y)
}

func outName(reduceNum int) string {
	return fmt.Sprintf("mr-out%d", reduceNum)
}
