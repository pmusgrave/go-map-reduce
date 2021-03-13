package mr

//import "fmt"
import "log"
import "net"
import "os"
import "path/filepath"
import "net/rpc"
import "net/http"
import "strconv"

// import "strings"
import "sync"
import "time"

type Task struct {
	id       int
	filename string
	status   string
	start    time.Time
}

type Master struct {
	mapch         chan Task
	map_done      bool
	map_status    map[int]Task
	mu            sync.Mutex
	nReduce       int
	reducech      chan Task
	reduce_done   bool
	reduce_set    map[int]struct{}
	reduce_status map[int]Task
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args *Args, reply *Reply) error {
	m.UpdateMapStatus()
	m.UpdateReduceStatus()
	m.mu.Lock()
	map_done := m.map_done
	reduce_done := m.reduce_done
	m.mu.Unlock()
	if len(m.mapch) > 0 || !map_done {
		for len(m.mapch) == 0 {
			time.Sleep(1000 * time.Millisecond)
		}
		m.mu.Lock()
		task := <-m.mapch
		reply.Id = task.id
		reply.Type = "map"
		// fmt.Printf("Assigning %s to map worker\n", task.filename)
		reply.Filename = task.filename
		reply.NReduce = m.nReduce

		task.status = "in-progress"
		task.start = time.Now()
		m.map_status[task.id] = task

		m.mu.Unlock()
	} else if len(m.reducech) > 0 || !reduce_done {
		for m.MapTasksInProgress() {
			time.Sleep(1000 * time.Millisecond)
		}
		for len(m.reducech) == 0 {
			time.Sleep(1000 * time.Millisecond)
		}

		m.mu.Lock()
		task := <-m.reducech
		s := "mr-*-" + strconv.Itoa(task.id) + ".txt"
		file_list, _ := filepath.Glob(s)
		task.status = "in-progress"
		reply.Id = task.id
		reply.Type = "reduce"
		reply.FileList = file_list
		//fmt.Printf("Assigning %s to reduce worker\n", task.filename)
		reply.Filename = s
		m.mu.Unlock()
	} else {
		reply.Type = "exit"
		reply.Filename = "exit"
		reply.Id = -1
	}

	return nil
}

func (m *Master) CompleteMapTask(args *CompleteMapArgs, reply *Reply) error {
	m.mu.Lock()
	// fmt.Printf("Task %d complete\n", args.Id)
	task := m.map_status[args.Id]
	task.status = "completed"
	m.map_status[args.Id] = task
	m.mu.Unlock()
	m.UpdateMapStatus()
	return nil
}

func (m *Master) AddReduceTask(args *ReduceArgs, reply *ReduceReply) error {
	m.mu.Lock()
	n_before := len(m.reduce_set)
	m.reduce_set[args.Id] = struct{}{}
	n_after := len(m.reduce_set)
	if n_after != n_before {
		task := m.reduce_status[args.Id]
		m.reducech <- task
	}
	m.mu.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
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
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
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
	//map_in_progress := m.MapTasksInProgress()
	m.UpdateMapStatus()
	m.UpdateReduceStatus()
	m.mu.Lock()
	for _, task := range m.map_status {
		if task.status == "in-progress" {
			ts := time.Since(task.start)
			if ts > 10000*time.Millisecond {
				//fmt.Printf("Map task %d timed out, diff=%d.\n", task.id, ts)
				task.status = "idle"
				m.map_status[task.id] = task
				m.mapch <- task
				m.mu.Unlock()
				return false
			}
		}
	}
	for _, task := range m.reduce_status {
		if task.status == "in-progress" {
			ts := time.Since(task.start)
			if ts > 10000*time.Millisecond {
				//fmt.Printf("Reduce task %d timed out, diff=%d.\n", task.id, ts)
				task.status = "idle"
				m.map_status[task.id] = task
				m.mapch <- task
				m.mu.Unlock()
				return false
			}
		}
	}
	m.mu.Unlock()
	//return !map_in_progress && len(m.mapch) == 0 && len(m.reducech) == 0
	return len(m.mapch) == 0 && len(m.reducech) == 0
}

func (m *Master) MapTasksInProgress() bool {
	m.mu.Lock()
	for _, task := range m.map_status {
		if task.status != "completed" {
			// fmt.Printf("%d %s\n", task.id, task.status)
			m.mu.Unlock()
			return true
		}
	}
	m.mu.Unlock()
	return false
}

func (m *Master) UpdateMapStatus() {
	m.mu.Lock()
	m.map_done = true
	for _, task := range m.map_status {
		if task.status != "completed" {
			m.map_done = false
			// fmt.Printf("%d %s\n", task.id, task.status)
		}
	}
	m.mu.Unlock()
}

func (m *Master) UpdateReduceStatus() {
	m.mu.Lock()
	m.reduce_done = true
	for _, task := range m.reduce_status {
		if task.status != "completed" {
			m.reduce_done = false
			// fmt.Printf("%d %s\n", task.id, task.status)
		}
	}
	m.mu.Unlock()
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.nReduce = nReduce
	m.map_done = false
	m.map_status = make(map[int]Task)
	m.mapch = make(chan Task, nReduce)
	m.reducech = make(chan Task, nReduce)
	m.reduce_set = make(map[int]struct{})
	m.reduce_status = make(map[int]Task)

	for n, f := range files {
		map_task := Task{
			id:       n,
			filename: f,
			status:   "idle",
			start:    time.Now(),
		}
		m.mapch <- map_task
	}
	for i := 0; i < nReduce; i++ {
		s := "mr-*-" + strconv.Itoa(i) + ".txt"
		reduce_task := Task{
			id:       i,
			filename: s,
			status:   "idle",
			start:    time.Now(),
		}
		m.reduce_status[i] = reduce_task
	}

	m.server()
	return &m
}
