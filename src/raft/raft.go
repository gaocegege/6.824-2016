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
	"log"
	"math/rand"
	"sync"
	"time"
)

// import "bytes"
// import "encoding/gob"

// Role represents the role of a Raft peer
type Role int

const (
	// Follower role
	Follower Role = iota
	// Candidate role
	Candidate
	// Leader role
	Leader
)

var (
	// MinimumElectionTimeoutMS can be set at package initialization. It may be
	// raised to achieve more reliable replication in slow networks, or lowered
	// to achieve faster replication in fast networks. Lowering is not
	// recommended.
	MinimumElectionTimeoutMS int32 = 2000

	maximumElectionTimeoutMS = 2 * MinimumElectionTimeoutMS
)

// electionTimeout returns a variable time.Duration, between the minimum and
// maximum election timeouts.
func electionTimeout() time.Duration {
	n := rand.Intn(int(maximumElectionTimeoutMS - MinimumElectionTimeoutMS))
	d := int(MinimumElectionTimeoutMS) + n
	return time.Duration(d) * time.Millisecond
}

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	// Persistent state on all servers
	currentTerm int
	votedFor    int
	//logs []byte

	// Volatile state on all servers
	commitIndex int
	lastApplied int

	//Leader state will reinit on election.
	nextIndex  []int
	matchIndex []int

	// defined for ease of implementation
	role         Role
	eventBus     chan *event
	electionTick <-chan time.Time
}

type event struct {
	peer        int
	requestType interface{}
}

// GetState return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.role == Leader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

//
// RequestVoteArgs is a RPC arguments structure.
//
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// RequestVoteReply is a RPC reply structure.
//
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

//
// RequestVote will give vote
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
	}

	rf.currentTerm = args.Term
	reply.Term = args.Term

	// buggy
	if rf.votedFor == -1 || rf.votedFor == args.CandidateID {
		reply.VoteGranted = true
		rf.votedFor = args.CandidateID
	} else {
		reply.VoteGranted = false
	}

	rf.eventBus <- &event{
		peer:        rf.me,
		requestType: &args,
	}
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
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	log.Println("Start a command.")
	index := -1
	term := -1
	isLeader := true

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
	rf.eventBus = make(chan *event, 10)

	// Your initialization code here.
	rf.currentTerm = 0
	rf.votedFor = -1

	// Set the role to follower.
	rf.role = Follower
	// Set the timeout to work.
	rf.resetElectionTimeout()

	// Set the log.
	log.SetPrefix("[Raft]-")
	log.Printf("Start a new Raft peer %d.\n", rf.me)

	go rf.runServerLoop()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}

func (rf *Raft) runServerLoop() {
	for {
		switch rf.role {
		case Leader:
			rf.runLeaderLoop()
		case Candidate:
			rf.runCandidateLoop()
		case Follower:
			rf.runFollowerLoop()
		}
	}
}

func (rf *Raft) runFollowerLoop() {
	for rf.role == Follower {
		select {
		// Upgrade to candidate.
		case <-rf.electionTick:
			log.Printf("Peer %d upgrades to a candidate.", rf.me)
			rf.role = Candidate
			rf.currentTerm++
			rf.votedFor = -1
			return
		case e := <-rf.eventBus:
			switch e.requestType.(type) {
			case *RequestVoteArgs:
				// TODO
				rf.resetElectionTimeout()
				log.Printf("Peer %d receives RequestVote from %d.", rf.me, e.requestType.(*RequestVoteArgs).CandidateID)
				return
			}
		}
	}
}

func (rf *Raft) runCandidateLoop() {
	if rf.votedFor != -1 {
		log.Println("Error: voteFor is not -1 in candidate loop.")
	}

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			log.Printf("Peer %d sends RequestVote to %d.", rf.me, i)
			requestVoteReply := &RequestVoteReply{}
			ok := rf.sendRequestVote(i, RequestVoteArgs{
				Term:        rf.currentTerm,
				CandidateID: rf.me,
				// TODO
				LastLogIndex: -1,
				LastLogTerm:  -1,
			}, requestVoteReply)
			if !ok {
				log.Panic("Fail to send RequestVote to other peers.")
			}

			rf.eventBus <- &event{
				peer:        i,
				requestType: requestVoteReply,
			}
		}
	}

	log.Printf("Peer %d receives all the RequestVoteReplys.", rf.me)
	var votes = map[int]bool{rf.me: true}

	for rf.role == Candidate {
		select {
		case <-rf.electionTick:
			log.Printf("Peer %d starts a new election.", rf.me)
			rf.resetElectionTimeout()
			rf.currentTerm++
			rf.votedFor = -1
			return
		case e := <-rf.eventBus:
			switch e.requestType.(type) {
			case *RequestVoteReply:
				rf.handleRequestVoteReply(*e, &votes)
			}
		}
	}
}

func (rf *Raft) runLeaderLoop() {
	// TODO: heartbeat
}

// resetElectionTimeout will reset the timeout chan.
func (rf *Raft) resetElectionTimeout() {
	rf.electionTick = time.NewTimer(electionTimeout()).C
}

func (rf *Raft) handleRequestVoteReply(e event, votes *map[int]bool) {
	var reply = e.requestType.(*RequestVoteReply)
	if reply.Term > rf.currentTerm {
		rf.role = Follower
		rf.votedFor = -1
	}

	if reply.Term < rf.currentTerm {
		return
	}

	if reply.VoteGranted {
		(*votes)[e.peer] = true
	}
	var count = 0
	for _, voteGranted := range *votes {
		if voteGranted {
			count++
		}
	}

	if count > len(rf.peers)/2 {
		log.Printf("Peer %d has become the new leader. Cong!", rf.me)
		rf.role = Leader
		rf.votedFor = rf.me
	}
}

// func (rf *Raft) requestVoteHandler(rv RequestVoteArgs) (*RequestVoteReply, bool) {
// 	// If the request is from an old term, reject.
// 	if rv.Term < rf.currentTerm {
// 		return &RequestVoteReply {
// 			Term: rf.currentTerm,
// 			VoteGranted: false,
// 		}, false
// 	}

// 	stepDown := false
// 	if rv.Term > rf.currentTerm {
// 		rf.currentTerm = rv.Term
// 		rf.votedFor = -1
// 		stepDown = true
// 	}

// 	if rf.role == Leader && stepDown == false {
// 		return &RequestVoteReply {
// 			Term: rf.currentTerm,
// 			VoteGranted: false,
// 		}, false
// 	}
// }
