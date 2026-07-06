package raft

type LogEntry struct {
	info    string
	version int
	term    int
	Command interface{}
}
