package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"log"
	"math/rand"
	"net/http"
	"os"
	"strconv"
	"sync"
	"time"
)

// ── Types ─────────────────────────────────────────────────────

type LogEntry struct {
	Index     int64  `json:"index"`
	Term      int64  `json:"term"`
	Key       string `json:"key"`
	Value     string `json:"value"`
	Operation string `json:"operation"` // SET | DELETE
}

type NodeState string

const (
	Follower  NodeState = "FOLLOWER"
	Candidate NodeState = "CANDIDATE"
	Leader    NodeState = "LEADER"
)

type Node struct {
	mu sync.RWMutex

	ID    string
	Port  int
	Peers []string

	State       NodeState
	CurrentTerm int64
	VotedFor    string
	LeaderID    string

	Log         []LogEntry
	CommitIndex int64
	LastApplied int64

	Store map[string]string

	lastHeartbeat   time.Time
	electionTimeout time.Duration
}

type VoteRequest struct {
	Term         int64  `json:"term"`
	CandidateID  string `json:"candidate_id"`
	LastLogIndex int64  `json:"last_log_index"`
	LastLogTerm  int64  `json:"last_log_term"`
}

type VoteResponse struct {
	Term        int64  `json:"term"`
	VoteGranted bool   `json:"vote_granted"`
	VoterID     string `json:"voter_id"`
}

type AppendEntriesRequest struct {
	Term         int64      `json:"term"`
	LeaderID     string     `json:"leader_id"`
	PrevLogIndex int64      `json:"prev_log_index"`
	PrevLogTerm  int64      `json:"prev_log_term"`
	Entries      []LogEntry `json:"entries"`
	LeaderCommit int64      `json:"leader_commit"`
}

type AppendEntriesResponse struct {
	Term    int64  `json:"term"`
	Success bool   `json:"success"`
	NodeID  string `json:"node_id"`
}

type WriteRequest struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Operation string `json:"operation"`
}

type StatusResponse struct {
	NodeID      string            `json:"node_id"`
	State       NodeState         `json:"state"`
	Term        int64             `json:"term"`
	LeaderID    string            `json:"leader_id"`
	Peers       []string          `json:"peers"`
	LogSize     int               `json:"log_size"`
	CommitIndex int64             `json:"commit_index"`
	Store       map[string]string `json:"store"`
	Uptime      string            `json:"uptime"`
}

// ── Globals ───────────────────────────────────────────────────

var (
	node      *Node
	startTime = time.Now()
)

// ── Node init ─────────────────────────────────────────────────

func newNode(id string, port int, peers []string) *Node {
	return &Node{
		ID:              id,
		Port:            port,
		Peers:           peers,
		State:           Follower,
		CurrentTerm:     0,
		Store:           make(map[string]string),
		Log:             []LogEntry{},
		lastHeartbeat:   time.Now(),
		electionTimeout: randomTimeout(),
	}
}

func randomTimeout() time.Duration {
	return time.Duration(1500+rand.Intn(1500)) * time.Millisecond
}

// ── Raft ──────────────────────────────────────────────────────

func (n *Node) run() {
	go n.electionLoop()
	go n.applyLoop()
}

func (n *Node) electionLoop() {
	for {
		time.Sleep(100 * time.Millisecond)
		n.mu.Lock()
		state := n.State
		since := time.Since(n.lastHeartbeat)
		timeout := n.electionTimeout
		n.mu.Unlock()
		if state != Leader && since > timeout {
			n.startElection()
		}
	}
}

func (n *Node) applyLoop() {
	for {
		time.Sleep(50 * time.Millisecond)
		n.mu.Lock()
		for n.CommitIndex > n.LastApplied {
			n.LastApplied++
			if int(n.LastApplied) <= len(n.Log) {
				e := n.Log[n.LastApplied-1]
				if e.Operation == "SET" {
					n.Store[e.Key] = e.Value
				} else if e.Operation == "DELETE" {
					delete(n.Store, e.Key)
				}
				log.Printf("[%s] applied %s %s=%s (idx=%d)", n.ID, e.Operation, e.Key, e.Value, n.LastApplied)
			}
		}
		n.mu.Unlock()
	}
}

func (n *Node) startElection() {
	n.mu.Lock()
	n.State = Candidate
	n.CurrentTerm++
	n.VotedFor = n.ID
	n.lastHeartbeat = time.Now()
	n.electionTimeout = randomTimeout()
	term := n.CurrentTerm
	lastIdx := int64(len(n.Log))
	var lastTerm int64
	if lastIdx > 0 {
		lastTerm = n.Log[lastIdx-1].Term
	}
	peers := n.Peers
	n.mu.Unlock()

	log.Printf("[%s] election → term %d", n.ID, term)

	votes := 1
	var wg sync.WaitGroup
	var mu sync.Mutex

	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			resp, err := sendJSON[VoteResponse](peer+"/vote", VoteRequest{
				Term: term, CandidateID: n.ID,
				LastLogIndex: lastIdx, LastLogTerm: lastTerm,
			})
			if err != nil {
				return
			}
			n.mu.Lock()
			if resp.Term > n.CurrentTerm {
				n.CurrentTerm = resp.Term
				n.State = Follower
				n.VotedFor = ""
				n.mu.Unlock()
				return
			}
			n.mu.Unlock()
			if resp.VoteGranted {
				mu.Lock()
				votes++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	n.mu.Lock()
	defer n.mu.Unlock()
	if n.State == Candidate && n.CurrentTerm == term {
		majority := (len(peers)+1)/2 + 1
		if votes >= majority {
			log.Printf("[%s] became LEADER (term %d, votes %d)", n.ID, term, votes)
			n.State = Leader
			n.LeaderID = n.ID
			go n.leaderLoop()
		} else {
			n.State = Follower
		}
	}
}

func (n *Node) leaderLoop() {
	for {
		n.mu.RLock()
		if n.State != Leader {
			n.mu.RUnlock()
			return
		}
		peers := n.Peers
		term := n.CurrentTerm
		commit := n.CommitIndex
		logLen := int64(len(n.Log))
		var prevTerm int64
		if logLen > 0 {
			prevTerm = n.Log[logLen-1].Term
		}
		n.mu.RUnlock()

		for _, peer := range peers {
			go func(peer string) {
				resp, err := sendJSON[AppendEntriesResponse](peer+"/append-entries", AppendEntriesRequest{
					Term: term, LeaderID: n.ID,
					PrevLogIndex: logLen, PrevLogTerm: prevTerm,
					Entries: []LogEntry{}, LeaderCommit: commit,
				})
				if err != nil {
					return
				}
				n.mu.Lock()
				if resp.Term > n.CurrentTerm {
					n.CurrentTerm = resp.Term
					n.State = Follower
					n.VotedFor = ""
				}
				n.mu.Unlock()
			}(peer)
		}
		time.Sleep(300 * time.Millisecond)
	}
}

func (n *Node) replicateEntry(entry LogEntry) bool {
	n.mu.Lock()
	n.Log = append(n.Log, entry)
	logIdx := int64(len(n.Log))
	peers := n.Peers
	term := n.CurrentTerm
	commit := n.CommitIndex
	var prevIdx, prevTerm int64
	if logIdx > 1 {
		prevIdx = logIdx - 1
		prevTerm = n.Log[prevIdx-1].Term
	}
	n.mu.Unlock()

	acks := 1
	var mu sync.Mutex
	var wg sync.WaitGroup

	for _, peer := range peers {
		wg.Add(1)
		go func(peer string) {
			defer wg.Done()
			resp, err := sendJSON[AppendEntriesResponse](peer+"/append-entries", AppendEntriesRequest{
				Term: term, LeaderID: n.ID,
				PrevLogIndex: prevIdx, PrevLogTerm: prevTerm,
				Entries: []LogEntry{entry}, LeaderCommit: commit,
			})
			if err != nil {
				return
			}
			if resp.Success {
				mu.Lock()
				acks++
				mu.Unlock()
			}
		}(peer)
	}
	wg.Wait()

	majority := (len(peers)+1)/2 + 1
	if acks >= majority {
		n.mu.Lock()
		n.CommitIndex = logIdx
		n.mu.Unlock()
		log.Printf("[%s] committed idx=%d (acks=%d)", n.ID, logIdx, acks)
		return true
	}
	return false
}

// ── Handlers ──────────────────────────────────────────────────

func cors(w http.ResponseWriter, r *http.Request) bool {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	if r.Method == "OPTIONS" {
		w.WriteHeader(204)
		return true
	}
	return false
}

func handleVote(w http.ResponseWriter, r *http.Request) {
	var req VoteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	node.mu.Lock()
	defer node.mu.Unlock()

	resp := VoteResponse{Term: node.CurrentTerm, VoterID: node.ID}
	if req.Term > node.CurrentTerm {
		node.CurrentTerm = req.Term
		node.State = Follower
		node.VotedFor = ""
	}
	lastIdx := int64(len(node.Log))
	var lastTerm int64
	if lastIdx > 0 {
		lastTerm = node.Log[lastIdx-1].Term
	}
	logOK := req.LastLogTerm > lastTerm || (req.LastLogTerm == lastTerm && req.LastLogIndex >= lastIdx)
	if req.Term >= node.CurrentTerm && (node.VotedFor == "" || node.VotedFor == req.CandidateID) && logOK {
		node.VotedFor = req.CandidateID
		node.lastHeartbeat = time.Now()
		resp.VoteGranted = true
	}
	json.NewEncoder(w).Encode(resp)
}

func handleAppendEntries(w http.ResponseWriter, r *http.Request) {
	var req AppendEntriesRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	node.mu.Lock()
	defer node.mu.Unlock()

	resp := AppendEntriesResponse{Term: node.CurrentTerm, NodeID: node.ID}
	if req.Term < node.CurrentTerm {
		json.NewEncoder(w).Encode(resp)
		return
	}

	node.lastHeartbeat = time.Now()
	node.electionTimeout = randomTimeout()
	if req.Term > node.CurrentTerm {
		node.CurrentTerm = req.Term
		node.VotedFor = ""
	}
	node.State = Follower
	node.LeaderID = req.LeaderID

	for _, entry := range req.Entries {
		idx := entry.Index - 1
		if idx >= 0 && idx < int64(len(node.Log)) && node.Log[idx].Term != entry.Term {
			node.Log = node.Log[:idx]
		}
		if entry.Index > int64(len(node.Log)) {
			node.Log = append(node.Log, entry)
		}
	}
	if req.LeaderCommit > node.CommitIndex {
		node.CommitIndex = min64(req.LeaderCommit, int64(len(node.Log)))
	}
	resp.Success = true
	json.NewEncoder(w).Encode(resp)
}

func handleWrite(w http.ResponseWriter, r *http.Request) {
	if cors(w, r) {
		return
	}
	var req WriteRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, err.Error(), 400)
		return
	}
	node.mu.RLock()
	state := node.State
	node.mu.RUnlock()

	if state != Leader {
		w.WriteHeader(http.StatusTemporaryRedirect)
		json.NewEncoder(w).Encode(map[string]string{"error": "not_leader"})
		return
	}

	node.mu.Lock()
	idx := int64(len(node.Log)) + 1
	entry := LogEntry{Index: idx, Term: node.CurrentTerm, Key: req.Key, Value: req.Value, Operation: req.Operation}
	node.mu.Unlock()

	if node.replicateEntry(entry) {
		json.NewEncoder(w).Encode(map[string]interface{}{"success": true, "index": idx})
	} else {
		w.WriteHeader(503)
		json.NewEncoder(w).Encode(map[string]interface{}{"success": false, "error": "no_quorum"})
	}
}

func handleRead(w http.ResponseWriter, r *http.Request) {
	cors(w, r)
	key := r.URL.Query().Get("key")
	node.mu.RLock()
	defer node.mu.RUnlock()
	val, ok := node.Store[key]
	if !ok {
		w.WriteHeader(404)
		json.NewEncoder(w).Encode(map[string]string{"error": "not_found"})
		return
	}
	json.NewEncoder(w).Encode(map[string]string{"key": key, "value": val})
}

func handleStatus(w http.ResponseWriter, r *http.Request) {
	cors(w, r)
	node.mu.RLock()
	defer node.mu.RUnlock()
	store := make(map[string]string)
	for k, v := range node.Store {
		store[k] = v
	}
	json.NewEncoder(w).Encode(StatusResponse{
		NodeID: node.ID, State: node.State, Term: node.CurrentTerm,
		LeaderID: node.LeaderID, Peers: node.Peers,
		LogSize: len(node.Log), CommitIndex: node.CommitIndex,
		Store: store, Uptime: time.Since(startTime).Round(time.Second).String(),
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	cors(w, r)
	w.Write([]byte("OK"))
}

// ── Helpers ───────────────────────────────────────────────────

func sendJSON[T any](url string, payload any) (T, error) {
	var zero T
	body, _ := json.Marshal(payload)
	client := &http.Client{Timeout: 500 * time.Millisecond}
	resp, err := client.Post("http://"+url, "application/json", bytes.NewReader(body))
	if err != nil {
		return zero, err
	}
	defer resp.Body.Close()
	var result T
	return result, json.NewDecoder(resp.Body).Decode(&result)
}

func min64(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// ── Main ──────────────────────────────────────────────────────

func main() {
	rand.Seed(time.Now().UnixNano())

	portStr := os.Getenv("PORT")
	if portStr == "" {
		portStr = "8001"
	}
	port, _ := strconv.Atoi(portStr)

	nodeID := os.Getenv("NODE_ID")
	if nodeID == "" {
		nodeID = fmt.Sprintf("node-%d", port)
	}

	var peers []string
	if p := os.Getenv("PEERS"); p != "" {
		json.Unmarshal([]byte(p), &peers)
	}

	node = newNode(nodeID, port, peers)
	node.run()

	mux := http.NewServeMux()
	mux.HandleFunc("/vote", handleVote)
	mux.HandleFunc("/append-entries", handleAppendEntries)
	mux.HandleFunc("/write", handleWrite)
	mux.HandleFunc("/read", handleRead)
	mux.HandleFunc("/status", handleStatus)
	mux.HandleFunc("/health", handleHealth)

	log.Printf("[%s] :%d peers=%v", nodeID, port, peers)
	log.Fatal(http.ListenAndServe(fmt.Sprintf(":%d", port), mux))
}
