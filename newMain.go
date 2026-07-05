package main

import (
	"fmt"
	"math/rand"
	"sync"
	"time"
)

const pi = 3.14
const MaxRetries int = 3

const (
	Follower = iota
	Candidate
	Leader
)

type Raft struct {
	mu          sync.Mutex
	currentTerm int // ← Raft 的"时钟"
	votedFor    int // ← 本 term 投了谁(-1 = 没投)

	commitIndex int // ← 已提交到哪
	role        int // ← Follower/Candidate/Leader,就是你说的 iota 那段
	// ... 还有 nextIndex[] / matchIndex[] (Leader 专用)
}

type RequestVote struct {
	votedFor    int
	currentTerm int
	newTerm     int
}

const (
	debug = iota
	info
	warn
	error
)
const (
	_  = iota
	KB = 1 << (10 * iota)
	MB
	GB
)

/*
	func main() {
		var x int = 100
		fmt.Println(x)

		var y = "hello"
		fmt.Println(y)

		z := 3.14
		fmt.Println(z)

		fmt.Println(pi)
		fmt.Println(Follower)
		rf := &Raft{currentTerm: 5, role: Leader}
		s := print(rf)
		fmt.Println(s)
		test()
	}
*/
func test() {
	var s string = "hello world"
	fmt.Println(s)
	var raw string = "22333333333333333333m33333"
	fmt.Println(raw)
	var arr [3]string = [3]string{"a", "b", "c"}
	fmt.Println(arr)
	person := &Person{age: 1, name: "哈哈"}
	fmt.Println(person)

	i := max(1, 2)
	fmt.Println(i)

	go func() {
		fmt.Println("become follower")
	}()
	time.Sleep(time.Second)

	//demoWaitGroup()
	demoChannel()
	demoSelectTimeout()
	rs := &RaftState{term: 5}
	rs.BecomeFollower(6)
	fmt.Println(rs.term)

	currentTerm := 5
	for ll := 0; ll < 8; ll++ {
		go func(ll int) {
			action, newTerm := compareTerm(currentTerm, ll)
			fmt.Println("action :=", action, "newTerm :=", newTerm)
		}(ll)
	}
	time.Sleep(time.Second)
}
func print(rf *Raft) string {
	return fmt.Sprintf("term=%d role=%s votedFor=%d commit=%d",
		rf.currentTerm, roleName(rf.role), rf.votedFor, rf.commitIndex)
}
func roleName(r int) string {
	return [...]string{"Follower", "Candidate", "Leader"}[r]
}

func (r *Raft) Greee() {
	r.mu.Lock()
	defer r.mu.Unlock()
}

type Greeter interface {
	Greet() string
}

func (p Person) String() string {
	return fmt.Sprintf("%s %s", p.name, p.age)
}

type Person struct {
	name string
	age  int
}

func (p Person) Greet() string {
	return "Hello " + p.name
}

func max(a, b int) int {
	if a > b {
		return a
	}
	return b
}

type RaftState struct {
	mu       sync.Mutex
	term     int
	votedFor int
	log      []string
}

func BecomeFollower(newTerm int) int {
	return newTerm
}

func (rs *RaftState) BecomeFollower(newTerm int) {
	rs.term = newTerm

}

func demoWaitGroup() {
	var wg sync.WaitGroup
	peers := []string{"a", "b", "c"}
	for _, peer := range peers {
		wg.Add(1)
		go func(p string) {
			defer wg.Done()
			time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
			fmt.Println("WaitGroup", p, "done")
		}(peer)
	}
	wg.Wait()
	fmt.Println("All goroutines done")
}

func demoChannel() {
	votech := make(chan bool, 3)
	for i := 1; i <= 3; i++ {
		go func(id int) {
			time.Sleep(2 * time.Millisecond)
			votech <- true
			fmt.Println("Channel", id, "done")
		}(i)
	}
	votes := 0
	for i := 0; i < 3; i++ {
		<-votech
		votes++
	}
	fmt.Println("channel", votes, "votes", votes)
}

func demoSelectTimeout() {
	ch := make(chan string)
	go func() {
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		ch <- "slow reply"
	}()
	select {
	case msg := <-ch:
		fmt.Println(msg)

	case <-time.After(time.Duration(rand.Intn(100)) * time.Millisecond):
		fmt.Println("timeout")
	}
}

// 比的是 RPC 传过来的 term，不是当前 term。 对方比你小就拒，比你大就退，一样就接
func compareTerm(currentTerm, rpcTerm int) (action string, newTerm int) {
	/*if currentTerm == rpcTerm {
		fmt.Println("current term is the same as rpc term", rpcTerm)
		return "reject", currentTerm
	}
	if currentTerm > rpcTerm {
		fmt.Println("current term is the same as rpc term", rpcTerm)
		return "step_down", rpcTerm
	}
	return "step_up", currentTerm*/
	if rpcTerm < currentTerm {
		return "reject", currentTerm
	}
	if rpcTerm > currentTerm {
		return "step_down", rpcTerm
	}
	return "accept", currentTerm
}

// 发送投票进行选举
// 1 数据源 就是谁投得 2 投票这里其实个状态 0 是投 1 是不  3 是否成功
func sendRpc(req *RequestVote) (action string, newTerm int) {

	ch := make(chan bool, 1)
	go func() {
		time.Sleep(time.Duration(rand.Intn(500)) * time.Millisecond) // 模拟网络延迟
		ch <- true
	}()
	select {
	case <-ch:
		return "success", req.newTerm
	case <-time.After(300 * time.Millisecond):
		return "timeout", req.currentTerm
	}
	/*
		sendRpcChannel := make(chan bool, req.votedFor)
		go func(vote *RequestVote) {
			time.Sleep(300 * time.Millisecond)
			sendRpcChannel <- true
		}(req)
		//
		reply := <-sendRpcChannel
		fmt.Println(reply)
		return action, req.votedFor*/
}

func lockAndUpdate(rfState *RaftState, newTerm int, newVote int) (term int, vote int) {
	// 这里的mu 需要在 RaftState 显示的声明一下
	fmt.Println("newTerm", newTerm, "newVote", newVote)
	rfState.mu.Lock()
	defer rfState.mu.Unlock()
	rfState.votedFor = newVote
	rfState.term = newTerm
	fmt.Println("lockAndUpdate", rfState)
	//rfState.mu.Unlock()
	return rfState.term, rfState.votedFor
}

func forSelect() {
	ch := make(chan string)
	go func() {
		time.Sleep(time.Duration(rand.Intn(100)) * time.Millisecond)
		ch <- "slow reply"
	}()

	for {
		select {
		case msg := <-ch:
			fmt.Println(msg)
			return

		case <-time.After(time.Duration(rand.Intn(500)) * time.Millisecond):
			fmt.Println("timeout")
			return
		}
	}
}
