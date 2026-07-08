package raft

import (
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
	nextIndex     []int        // Leader：每个 Follower 下一条该发的 index
	matchIndex    []int        // Leader：每个 Follower 已匹配的最高 index
	commitIndex   int          // 已提交的最高 index
	lastApplied   int          // 已应用到状态机的最高 index
	hbLock        []sync.Mutex // 每个 follower 一个心跳锁，防止积压
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
	Term         int
	LeaderId     int
	PrevLogIndex int        // 新条目前一条的 index
	PrevLogTerm  int        // 新条目前一条的 term
	Entries      []LogEntry // 要复制的日志（心跳时为空）
	LeaderCommit int        // Leader 的 commitIndex
}
type AppendEntriesReply struct {
	Term          int
	LeaderId      int
	Success       bool
	ConflictIndex int // 冲突的日志
	ConflictTerm  int // 冲突的任期
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
	//fmt.Print(rf.currentTerm, " ", args.Term, " ", args.CandidateId)
	if args.Term > rf.currentTerm {
		//fmt.Println("vote granted")
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
	//候选人最后一条日志 term 比我的大 → 比我新
	//term 相等但 index ≥ 我的 → 比我新
	//否则 → 拒绝
	lastLogIndex := len(rf.log) - 1
	lastLogTerm := 0
	if lastLogIndex >= 0 {
		lastLogTerm = rf.log[lastLogIndex].Term
	}
	logOk := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	if !logOk {
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

	// 来自合法 Leader 的 RPC，重置选举计时器（即使日志不匹配也不超时选主）
	rf.lastHeartbeat = time.Now()

	// PrevLogIndex 的边界检查
	if args.PrevLogIndex >= 0 && args.PrevLogIndex < len(rf.log) {
		if rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
			reply.Success = false
			reply.Term = rf.currentTerm
			reply.ConflictTerm = rf.log[args.PrevLogIndex].Term
			for i := args.PrevLogIndex; i >= 0; i-- {
				if rf.log[i].Term != reply.ConflictTerm {
					reply.ConflictIndex = i + 1
					break
				}
			}
			return
		}
	} else if args.PrevLogIndex >= len(rf.log) {
		reply.Success = false
		reply.Term = rf.currentTerm
		reply.ConflictIndex = len(rf.log)
		return
	}

	reply.Success = true
	reply.Term = rf.currentTerm
	// 发送日志
	for i, entry := range args.Entries {
		idx := args.PrevLogIndex + i + 1
		if idx < len(rf.log) {
			if rf.log[idx].Term != entry.Term {
				rf.log = rf.log[:idx]
			} else {
				continue
			}
		}
		rf.log = append(rf.log, entry)
	}
	// 截断 follower 日志中超出 leader 发送范围的过时条目
	rf.log = rf.log[:args.PrevLogIndex+1+len(args.Entries)]

	// commitIndex 更新移到 entries 追加之后，使用 leader 发送的最后一个 index
	lastNewEntry := args.PrevLogIndex + len(args.Entries)
	commitNode := min(args.LeaderCommit, lastNewEntry)
	if commitNode > rf.commitIndex {
		rf.commitIndex = commitNode
	}
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
		rf.mu.Lock()
		if rf.role == Leader {
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		} else {
			rf.mu.Unlock()
			ms := 300 + (rand.Int63() % 300)
			time.Sleep(time.Duration(ms) * time.Millisecond)
		}
		rf.mu.Lock()
		if rf.role != Leader && time.Since(rf.lastHeartbeat) > time.Duration(300)*time.Millisecond {
			rf.role = Candidate
			rf.currentTerm++
			rf.votedFor = rf.me
			electionTerm := rf.currentTerm
			lastLogTerm := 0
			if len(rf.log) > 0 {
				lastLogTerm = rf.log[len(rf.log)-1].Term
			}
			args := RequestVoteArgs{
				Term:         rf.currentTerm,
				CandidateId:  rf.me,
				LastLogIndex: len(rf.log) - 1,
				LastLogTerm:  lastLogTerm, // 2B 再补
			}
			rf.mu.Unlock()
			votes := 1
			var mu sync.Mutex
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go func(server int) {
					reply := RequestVoteReply{}
					ok := rf.sendRequestVote(server, &args, &reply)
					if !ok {
						return
					}
					rf.mu.Lock()
					if reply.Term > rf.currentTerm {
						rf.currentTerm = reply.Term
						rf.role = Follower
						rf.votedFor = -1
						rf.mu.Unlock()
						return
					}
					if reply.VoteGranted && rf.role == Candidate && rf.currentTerm == electionTerm {
						votes++
					}
					rf.mu.Unlock()
					mu.Lock()
					if votes > len(rf.peers)/2 {
						rf.mu.Lock()
						if rf.role == Candidate && rf.currentTerm == electionTerm {
							rf.role = Leader
							rf.nextIndex = make([]int, len(rf.peers))
							rf.matchIndex = make([]int, len(rf.peers))
							for i := range rf.nextIndex {
								rf.nextIndex[i] = len(rf.log)
							}
							rf.mu.Unlock()
							// 立即发送心跳，不等所有 RPC 返回
							for j := 0; j < len(rf.peers); j++ {
								if j == rf.me {
									continue
								}
								go rf.sendHeartbeat(j)
							}
						} else {
							rf.mu.Unlock()
						}
					}
					mu.Unlock()
				}(i)
			}
			// 等待一小段时间，让选举结果稳定
			time.Sleep(50 * time.Millisecond)
			rf.mu.Lock()
			if rf.role == Leader {
				rf.mu.Unlock()
				// 立即发一轮心跳
				for j := 0; j < len(rf.peers); j++ {
					if j == rf.me {
						continue
					}
					go rf.sendHeartbeat(j)
				}
			} else {
				rf.mu.Unlock()
			}
			continue
		} else if rf.role == Leader {
			rf.mu.Unlock()
			for i := 0; i < len(rf.peers); i++ {
				if i == rf.me {
					continue
				}
				go rf.sendHeartbeat(i)
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
		log:         make([]LogEntry, 1),
		hbLock:      make([]sync.Mutex, len(peers)),
	}
	// 初始化持久化状态
	rf.readPersist(persister.ReadRaftState())
	go rf.ticker()
	go func() {
		for {
			rf.mu.Lock()
			for rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				msg := ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.lastApplied,
					Command:      rf.log[rf.lastApplied].Command,
				}
				rf.mu.Unlock()
				rf.applyCh <- msg
				rf.mu.Lock()
			}
			rf.mu.Unlock()
			time.Sleep(100 * time.Millisecond)
		}
	}()
	return rf
}

// Start
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.role != Leader {
		return -1, -1, false
	}
	rf.log = append(rf.log, LogEntry{Term: rf.currentTerm, Command: command})
	return len(rf.log) - 1, rf.currentTerm, true
}

// readPersist 从持久化数据恢复状态
func (rf *Raft) readPersist(data []byte) {

}

// 发送心跳日志
func (rf *Raft) sendHeartbeat(server int) {
	rf.mu.Lock()
	if rf.role != Leader {
		rf.mu.Unlock()
		return
	}
	nextIndex := rf.nextIndex[server]
	prevLogIndex := nextIndex - 1
	// 若 prevLogIndex 超出 Leader 日志范围（follower 日志比 leader 长），
	// 将 nextIndex 回退到 leader 日志末尾
	if prevLogIndex >= len(rf.log) {
		rf.nextIndex[server] = len(rf.log)
		nextIndex = rf.nextIndex[server]
		prevLogIndex = nextIndex - 1
	}
	var prevLogTerm int
	if prevLogIndex >= 0 && prevLogIndex < len(rf.log) {
		prevLogTerm = rf.log[prevLogIndex].Term
	}
	// 快照 entries，避免在锁内构建大切片
	logLen := len(rf.log)
	entries := make([]LogEntry, logLen-nextIndex)
	copy(entries, rf.log[nextIndex:])
	term := rf.currentTerm
	leaderCommit := rf.commitIndex
	rf.mu.Unlock()

	args := AppendEntriesArgs{
		Term:         term,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      entries,
		LeaderCommit: leaderCommit,
	}

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)
	if !ok {
		return
	}
	rf.mu.Lock()
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.role = Follower
		rf.votedFor = -1
		rf.mu.Unlock()
		return
	}
	if reply.Success {
		rf.matchIndex[server] = prevLogIndex + len(entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1
		for N := len(rf.log) - 1; N > rf.commitIndex; N-- {
			if rf.log[N].Term != rf.currentTerm {
				continue
			}
			count := 1
			for j := range rf.peers {
				if j != rf.me && rf.matchIndex[j] >= N {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = N
				break
			}
		}
	} else {
		if reply.ConflictTerm > 0 {
			lastIndexOfTerm := -1
			// 从当前 prevLogIndex 往前搜，确保 nextIndex 不会不减反增
			for i := rf.nextIndex[server] - 1; i >= 0; i-- {
				if rf.log[i].Term == reply.ConflictTerm {
					lastIndexOfTerm = i
					break
				}
			}
			if lastIndexOfTerm >= 0 {
				rf.nextIndex[server] = lastIndexOfTerm + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		} else {
			rf.nextIndex[server] = reply.ConflictIndex
		}
		if rf.nextIndex[server] < 1 {
			rf.nextIndex[server] = 1
		}
	}
	rf.mu.Unlock()
}
