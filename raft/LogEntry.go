package raft

type LogEntry struct {
	Info    string
	Version int
	Term    int
	Command interface{}
}
