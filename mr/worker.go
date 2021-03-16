package mr

import "encoding/json"
import "github.com/satori/go.uuid"
import "fmt"
import "io/ioutil"
import "log"
import "net/rpc"
import "hash/fnv"
import "os"
import "sort"
import "strconv"
import "strings"
import "time"

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func CompleteMapTask(id int) Reply {
	args := CompleteMapArgs{Id: id}
	reply := Reply{}
	call("Master.CompleteMapTask", &args, &reply)
	if reply.Filename != "" {
		return reply
	} else {
		reply.Filename = "error"
		return reply
	}
}

func CompleteReduceTask(id int) Reply {
	args := CompleteReduceArgs{Id: id}
	reply := Reply{}
	call("Master.CompleteReduceTask", &args, &reply)
	if reply.Filename != "" {
		return reply
	} else {
		reply.Filename = "error"
		return reply
	}
}

func GetTask() Reply {
	args := Args{}
	reply := Reply{}
	call("Master.GetTask", &args, &reply)
	if reply.Filename != "" {
		return reply
	} else {
		reply.Filename = "error"
		return reply
	}
}

func AddReduceTask(id int) ReduceReply {
	args := ReduceArgs{id}
	reply := ReduceReply{}
	call("Master.AddReduceTask", &args, &reply)
	return reply
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	reply := GetTask()
	id := reply.Id
	filename := reply.Filename

	for {
		if filename == "error" {
			//fmt.Printf("Error getting filename from master\n")
			//return
			time.Sleep(1000 * time.Millisecond)
			reply = GetTask()
			id = reply.Id
			filename = reply.Filename
		}
		// fmt.Printf("Worker received filename: %s\n", filename)

		var intermediate []KeyValue
		//intermediate := []KeyValue{}

		if reply.Type == "map" {
			file, err := os.Open(filename)
			if err != nil {
				//log.Fatalf("cannot open %v", filename)
				continue
			}
			content, err := ioutil.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", filename)
			}
			file.Close()

			kva := mapf(filename, string(content))
			intermediate = append(intermediate, kva...)
			WriteIntermediate(intermediate, id, reply.NReduce)
			CompleteMapTask(id)
		} else if reply.Type == "reduce" {
			for _, reduce_filename := range reply.FileList {
				file, err := os.Open(reduce_filename)
				if err != nil {
					//log.Fatalf("cannot open %v", reduce_filename)
					continue
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
				defer os.Remove(reduce_filename)
			}
			sort.Sort(ByKey(intermediate))
			// fmt.Println(intermediate)
			s := []string{"mr-out", "-", strconv.Itoa(reply.Id)}
			oname := strings.Join(s, "")
			// oname := "mr-out-0"
			ofile, _ := os.Create(oname)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
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
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}
			CompleteReduceTask(id)
		} else if reply.Type == "exit" {
			break
		}

		time.Sleep(1000 * time.Millisecond)
		reply = GetTask()
		id = reply.Id
		filename = reply.Filename
		intermediate = []KeyValue{}
	}

}

func WriteIntermediate(intermediate []KeyValue, worker_id int, n_reduce int) {
	initial_file_set := make(map[string][]KeyValue)
	file_list := make(map[string][]KeyValue)
	reduce_list := make(map[int]struct{})
	real_filenames := make(map[string]string)
	for _, pair := range intermediate {
		s := []string{"mr-", strconv.Itoa(worker_id), "-", strconv.Itoa(ihash(pair.Key) % n_reduce), ".txt"}
		filename := strings.Join(s, "")
		initial_file_set[filename] = append(initial_file_set[filename], pair)
	}
	for filename, pairs := range initial_file_set {
		tmp_uuid, uuid_err := uuid.NewV4()
		if uuid_err != nil {
			log.Fatal(uuid_err)
		}
		tmp_filename := tmp_uuid.String()
		/*tmp, err := ioutil.TempFile("", filename)
		if err != nil {
			log.Fatal(err)
		}*/
		file_list[tmp_filename] = pairs
		real_filenames[tmp_filename] = filename
		//tmp.Close()
	}
	for filename, pairs := range file_list {
		for _, pair := range pairs {
			f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
			if err != nil {
				log.Fatal(err)
			}

			reduce_list[ihash(pair.Key)%n_reduce] = struct{}{}

			enc := json.NewEncoder(f)
			enc_err := enc.Encode(&pair)
			if enc_err != nil {
				log.Fatal(enc_err)
			}

			f.Close()
			// defer os.Remove(filename)
		}
	}
	for reduce_task, _ := range reduce_list {
		AddReduceTask(reduce_task)
	}
	for filename, _ := range file_list {
		defer os.Rename(filename, real_filenames[filename])
	}
}

func WriteOutput(output []KeyValue, worker_id int) {
	for _, pair := range output {
		s := []string{"mr-out", "-", strconv.Itoa(worker_id), ".txt"}
		filename := strings.Join(s, "")
		f, err := os.OpenFile(filename, os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)

		if err != nil {
			log.Fatal(err)
		}

		line := pair.Key + " " + pair.Value
		_, err2 := f.WriteString(line)
		if err2 != nil {
			log.Fatal(err2)
		}

		f.Close()
	}
}

//
// example function to show how to make an RPC call to the master.
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
	sockname := masterSock()
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
