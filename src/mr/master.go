package mr

import (
	"fmt"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mu      sync.Mutex
	nMap    int
	nReduce int
	files   []string
	tasks   []TaskInfo
	cond    *sync.Cond
	phase   jobPhase
	done    bool
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	os.Remove("mr-socket")
	l, e := net.Listen("unix", "mr-socket")
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	// Your code here.

	return m.done
}

//
// create a Master.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nMap = len(files)
	m.nReduce = nReduce
	m.cond = sync.NewCond(&m.mu)
	m.files = files
	m.phase = MapPhase
	// Your code here.
	m.buildTaskList()
	go m.server()
	go m.checkProgress()
	return &m
}

func (m *Master) checkProgress() {
	running := true
	for running {
		m.mu.Lock()
		m.cond.Wait()
		switch m.phase {
		case MapPhase:
			if tasksCompleted(m.tasks) {
				m.phase = ReducePhase
				//log.Println("All Map tasks completed")
				m.buildTaskList() //build reduce task list
			}
		case ReducePhase:
			if tasksCompleted(m.tasks) {
				m.done = true
				running = false
				//log.Println("All Reduce tasks completed")
			}
		}
		m.mu.Unlock()
	}
}

func tasksCompleted(tasks []TaskInfo) bool {
	for i := range tasks {
		if tasks[i].done != true {
			return false
		}
	}
	return true
}

func (m *Master) buildTaskList() {

	switch m.phase {
	case MapPhase:
		m.tasks = make([]TaskInfo, m.nMap)
		for i, file := range m.files {
			task := &Task{TaskNum: i, Phase: MapPhase, FileName: file, OtherTaskNum: m.nReduce}
			m.tasks[i] = TaskInfo{task: task, handout: false, done: false}
		}
	case ReducePhase:
		m.tasks = make([]TaskInfo, m.nReduce)
		for i := range m.tasks {
			task := &Task{TaskNum: i, Phase: ReducePhase, OtherTaskNum: m.nMap}
			m.tasks[i] = TaskInfo{task: task, handout: false, done: false}
		}
	}
}

func (m *Master) getTask() (task *Task) {
	return getTask(m.tasks)
}

func getTask(taskInfos []TaskInfo) (task *Task) {
	for i := range taskInfos {
		if taskInfos[i].done {
			continue
		}
		if taskInfos[i].handout == false || time.Since(taskInfos[i].handoutTime).Seconds() > 5 {
			taskInfos[i].handout = true
			taskInfos[i].handoutTime = time.Now()
			return taskInfos[i].task
		}
	}
	return nil
}

func (m *Master) HandOutTask(_ *struct{}, reply *Task) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.done {
		reply.AllCompleted = true
		return nil
	}
	task := m.getTask()
	if task != nil {
		*reply = *task
		return nil
	}
	return ErrNoTask
}


func (m *Master) DoneTask(task *Task, _ *struct{}) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.taskDone(task)
	m.cond.Broadcast()
	return nil
}

func (m *Master) taskDone(task *Task) {
	m.tasks[task.TaskNum].done = true
}

var (
	ErrNoTask  = fmt.Errorf("no task")
	ErrAllDone = fmt.Errorf("all done")
)

// definitions
type Task struct {
	Phase        jobPhase
	FileName     string
	TaskNum      int
	OtherTaskNum int
	AllCompleted bool
}

type TaskInfo struct {
	task        *Task
	handout     bool
	handoutTime time.Time
	done        bool
}
type DoneReply struct {
	AllDone bool
}
type jobPhase int

const (
	MapPhase jobPhase = iota
	ReducePhase
)
func init(){
	log.SetFlags(log.Lshortfile)
}