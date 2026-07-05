package raft

import (
	"fmt"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	"learn-6.824/labrpc"
)

const (
	Follower = iota
	Candidate
	Leader
)

type ApplyMsg struct {
	CommandValid bool
	Command      interface{} // 这里定义了一个接口
	CommandIndex int
}

type Raft struct {
	mu            sync.Mutex
	persister     *Persister
	me            int
	dead          int32
	currentTerm   int // 当前任期编号
	votedFor      int // 当前term 给谁了(-1 没选)
	log           []LogEntry
	role          int // // Follower / Candidate / Leader 这里使用const
	peers         []*labrpc.ClientEnd
	applyCh       chan ApplyMsg
	lastHeartbeat time.Time
}
type RequestVoteArgs struct {
	Term         int // 候选人的 term
	CandidateId  int // 候选人 ID
	LastLogIndex int // 候选人最后一条日志 index
	LastLogTerm  int // 候选人最后一条日志 term
}
type RequestVoteReply struct {
	Term        int  //投票的当前term 编号
	VoteGranted bool // true 是投票了
}

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}
type AppendEntriesReply struct {
	Term     int
	LeaderId int
	Success  bool
}

/*
判断当前节点状态
*/
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	// Your code here (2A). 这里添加 是不是leader得判断
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == Leader
	return term, isleader
}

// 这段代码2c才会用到
func (rf *Raft) persist() {

}

/*
*

	1.收到请求
	2.比较当前任职编码 term得大小
	3.查看是否投过票 是不是给了当前机器 是->拒绝
	4.候选人的日志版本是不是大于我得日志版本 否->拒绝
	5. 投票
*/
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Print(rf.currentTerm, " ", args.Term, " ", args.CandidateId)
	if args.Term > rf.currentTerm {
		fmt.Println("vote granted")
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1
		// 这里退位了还能继续投标
		//return
	}
	// 2. 对方 term 比我大 → 退位，不 return，继续投票
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		return
	}
	// 3. 还没投票 or 已经投给这个人 → 投票
	if args.CandidateId == rf.votedFor || rf.votedFor == -1 {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
	} else {
		reply.VoteGranted = false
	}
	reply.Term = rf.currentTerm
	return
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	fmt.Print(rf.currentTerm, " ", args.Term, " ", args.LeaderId)
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.role = Follower
		rf.votedFor = -1

	}
	if args.Term < rf.currentTerm {
		reply.Success = false
		reply.Term = rf.currentTerm
		return
	}
	reply.Success = true
	//重置选举闹钟
	rf.lastHeartbeat = time.Now()
	reply.Term = rf.currentTerm
}

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}
func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// 如果我是 Leader，每 tick 给所有 Follower 发心跳（AppendEntries），维持统治。
// 如果我是 Follower，收到心跳就重置时钟，超时了就变 Candidate 选举。
// 选举时，收到投票请求的节点：
// 对方 term 大就退位投票，对方 term 小就拒绝
// 相等就按 votedFor 判断
func (rf *Raft) ticker() {
	for rf.killed() == false {
		ms := 300 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.lastHeartbeat) > 300*time.Millisecond {
			rf.role = Candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  0, // 2B 再补
			}
			rf.mu.Unlock()
			votes := 1
			var mu sync.Mutex
			var wg sync.WaitGroup
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				wg.Add(1)
				go func(server int) {
					defer wg.Done()
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(server, &args, &reply)
					rf.mu.Lock()
					if ok && reply.VoteGranted && rf.role == Candidate && rf.currentTerm == args.Term {
						votes++
					}
					rf.mu.Unlock()
					mu.Lock()
					if votes > len(rf.peers)/2 {
						rf.mu.Lock()
						rf.role = Leader
						rf.mu.Unlock()
					}
					mu.Unlock()
				}(i)
			}
			wg.Wait()
		} else if rf.role == Leader {
			args := AppendEntriesArgs{Term: rf.currentTerm, LeaderId: rf.me}
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(server int) {
					reply := AppendEntriesReply{}
					rf.sendAppendEntries(server, &args, &reply)
				}(i)
			}
		} else {
			rf.mu.Unlock()
		}
	}
}

func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// Make 创建 Raft 实例（测试框架入口，2A 先搭架子）
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{
		peers:       peers,
		persister:   persister,
		me:          me,
		applyCh:     applyCh,
		role:        Follower,
		currentTerm: 0,
		votedFor:    -1,
		log:         make([]LogEntry, 0),
	}
	// 初始化持久化状态
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	return rf
}

// Start 提交命令到日志（2B 实现）
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	return -1, -1, false
}

// readPersist 从持久化数据恢复状态
func (rf *Raft) readPersist(data []byte) {
}
