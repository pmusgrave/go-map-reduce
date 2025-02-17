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
	Y        int
	filename string
}

// Add your RPC definitions here.
type Args struct {
}

type Reply struct {
	Id       int
	Filename string
	FileList []string
	NReduce  int
	Type     string
}

type CompleteMapArgs struct {
	Id       int
	Filename string
}

type CompleteReduceArgs struct {
	Id       int
	Filename string
}

type ReduceArgs struct {
	Id int
}

type ReduceReply struct {
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
