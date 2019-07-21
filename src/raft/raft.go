package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

//日志条目结构
type LogEntry struct {
	term    int
	Command interface{}
}

//
type Role int

// 节点角色
const (
	Leader Role = iota
	Candidate
	Follower
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	currentTerm int
	votedFor    *labrpc.ClientEnd //投给谁
	log         []LogEntry

	commitIndex int
	lastApplied int

	//领导者使用的成员
	nextIndex  map[*labrpc.ClientEnd]int
	matchIndex map[*labrpc.ClientEnd]int

	role Role //标识节点当前的角色

	heatBeatCh chan bool //心跳channel
	votedCh    chan bool //投票channel

	//lastHeartBeatTime  time.Time
	//lastVotedTime time.Time
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	term = rf.currentTerm
	isleader = (rf.role == Leader)
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  *labrpc.ClientEnd
	LastLogIndex int //候选人的最后日志条目的索引值
	LastLogTerm  int //候选人最后日志条目的任期号
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  //当前任期号，以便于候选人去更新自己的任期号
	VoteGranted bool //候选人赢得了此张选票时为真
}

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	LeaderCommit int
	log          []LogEntry
}

//
type AppendEntriesReply struct {
	Term    int
	Success bool
}

//
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.heatBeatCh <- true
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.role = Follower
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else if len(args.log) == 0 {
		reply.Success = true
	} else if rf.log[args.PrevLogIndex].term != args.PrevLogTerm {
		reply.Success = false
	} else if rf.log[args.PrevLogIndex+1].term != args.Term {
		rf.log = rf.log[:args.PrevLogIndex+1]
		rf.log = append(rf.log, args.log[:])
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
		}
		if rf.commitIndex > rf.lastApplied {
			rf.lastApplied++
		}
		reply.Success = true
	}

}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.votedCh <- true

	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("request incoming.[method :RequestVote] CandidateID: %s, LastLogTerm : %d, LastLogIndex : %d,currentTerm : %d,currentLogLength : %d\n", args.CandidateID, args.LastLogTerm, args.LastLogIndex, len(rf.log))

	if args.Term < rf.currentTerm { //对方的任期号比自己的还要小
	} else if rf.votedFor == nil || rf.votedFor == args.CandidateID { //如果还没投过票 或者之前已经投过给他
		if args.LastLogTerm > rf.currentTerm {
			reply.VoteGranted = true
			rf.role = Follower
			DPrintf("Agree to vote. CandidateID : %s", args.CandidateID)
		} else if args.LastLogTerm == rf.currentTerm {
			if args.LastLogIndex >= len(rf.log) {
				reply.VoteGranted = true
				rf.role = Follower
				DPrintf("Agree to vote. CandidateID : %s", args.CandidateID)
				return
			}
		}
	}

	reply.VoteGranted = false
	reply.Term = rf.currentTerm
	DPrintf("Reject to vote. CandidateID : %s", args.CandidateID)
	return
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).

	return index, term, isLeader
}

//
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.role = Follower
	go func(raft *Raft) {
		random := rand.New(time.Now())
		for {

			if raft.role == Follower {
				timeout := 150 + random.Intn(150)
				ticker := time.NewTicker(timeout * time.Second)
				select {
				case <-rf.heatBeatCh:
					ticker.Stop()
				case <-rf.votedCh:
					ticker.Stop()
				case <-ticker.C:
					//timeout
					raft.mu.Lock()
					defer raft.mu.Unlock()
					raft.role = Candidate
					raft.votedFor = raft.peers[me]
				}
			}
			if raft.role == Candidate {
				raft.currentTerm++
				go func() {
					votedCount := 0
					for index, peer := range raft.peers {
						args := RequestVoteArgs{
							raft.currentTerm,
							raft.peers[me],
							len(raft.log) - 1,
							raft.log[len(raft.log)-1].term,
						}
						var reply RequestVoteReply
						if ok := raft.sendRequestVote(index, &args, &reply); !ok {
							continue
						}
						if reply.VoteGranted {
							votedCount++
						}
						if votedCount >= len(peers)/2+1 {
							raft.role = Leader
						}
					}
				}()
			}
			if raft.role == Leader {
				commitedCount := 0
				for index, peer := range raft.peers {
					if peer == raft.peers[me] {
						continue
					}
					args := AppendEntriesArgs{raft.currentTerm,
						raft.me,
						raft.nextIndex[peer],
						raft.log[raft.nextIndex[peer]].term,
						raft.commitIndex,
						nil,
					}
					var reply AppendEntriesReply
					if ok := rf.peers[index].Call("Raft.AppendEntries", &args, &reply); ok {
						if reply.Term == raft.currentTerm {
							if reply.Success {
								raft.nextIndex[peer] = len(raft.log) + 1
								raft.matchIndex[peer] = len(raft.log)
								if raft.matchIndex[peer] >= raft.commitIndex {
									commitedCount++
								}
							} else {
								raft.nextIndex[peer]--
							}
						}
					}
				}
				// commit
				if commitedCount > len(raft.peers)/2+1 && raft.log[len(raft.log)].term == raft.currentTerm {
					raft.commitIndex = len(raft.log)
				}
			}
		}
	}(rf)
	return rf
}
