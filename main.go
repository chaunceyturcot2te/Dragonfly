package main

import (
	"bufio"
	"flag"
	"fmt"
	"hash/fnv"
	"io"
	"net"
	"strconv"
	"strings"
	"sync"
	"time"
)

type KeyVal struct {
	Value     string
	ExpiresAt time.Time
}

const ShardCount = 8

type Shard struct {
	mu sync.RWMutex
	db map[string]KeyVal
}

type Database struct {
	shards [ShardCount]*Shard
}

func (db *Database) getShard(key string) *Shard {
	hasher := fnv.New32a()
	hasher.Write([]byte(key))
	return db.shards[hasher.Sum32()%ShardCount]
}

type Server struct {
	mu           sync.Mutex
	port         string
	db           *Database
	role         string // "master" or "replica"
	masterHost   string
	masterPort   string
	replicas     map[chan []string]bool
	replicaInbox chan []string
	replicaConn  net.Conn
	isPromoting  bool
}

func NewServer(port string) *Server {
	db := &Database{}
	for i := 0; i < ShardCount; i++ {
		db.shards[i] = &Shard{
			db: make(map[string]KeyVal),
		}
	}
	return &Server{
		port:     port,
		db:       db,
		role:     "master",
		replicas: make(map[chan []string]bool),
	}
}

func formatRESP(args []string) []byte {
	var sb strings.Builder
	sb.WriteString(fmt.Sprintf("*%d\r\n", len(args)))
	for _, arg := range args {
		sb.WriteString(fmt.Sprintf("$%d\r\n%s\r\n", len(arg), arg))
	}
	return []byte(sb.String())
}

func readCommand(reader *bufio.Reader) ([]string, error) {
	b, err := reader.ReadByte()
	if err != nil {
		return nil, err
	}
	err = reader.UnreadByte()
	if err != nil {
		return nil, err
	}
	if b == '*' {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		var count int
		_, err = fmt.Sscanf(line, "*%d\r\n", &count)
		if err != nil {
			_, err = fmt.Sscanf(line, "*%d\n", &count)
			if err != nil {
				return nil, err
			}
		}
		args := make([]string, count)
		for i := 0; i < count; i++ {
			line, err = reader.ReadString('\n')
			if err != nil {
				return nil, err
			}
			if len(line) == 0 || line[0] != '$' {
				return nil, fmt.Errorf("invalid bulk string header: %q", line)
			}
			var length int
			_, err = fmt.Sscanf(line, "$%d\r\n", &length)
			if err != nil {
				_, err = fmt.Sscanf(line, "$%d\n", &length)
				if err != nil {
					return nil, err
				}
			}
			buf := make([]byte, length+2)
			_, err = io.ReadFull(reader, buf)
			if err != nil {
				return nil, err
			}
			args[i] = string(buf[:length])
		}
		return args, nil
	} else {
		line, err := reader.ReadString('\n')
		if err != nil {
			return nil, err
		}
		line = strings.TrimSpace(line)
		if line == "" {
			return nil, nil
		}
		return strings.Fields(line), nil
	}
}

func (s *Server) get(key string) (string, bool) {
	shard := s.db.getShard(key)
	shard.mu.Lock()
	defer shard.mu.Unlock()

	kv, ok := shard.db[key]
	if !ok {
		return "", false
	}
	if !kv.ExpiresAt.IsZero() && kv.ExpiresAt.Before(time.Now()) {
		delete(shard.db, key)
		s.mu.Lock()
		isMaster := s.role == "master"
		s.mu.Unlock()
		if isMaster {
			s.propagate([]string{"DEL", key})
		}
		return "", false
	}
	return kv.Value, true
}

func (s *Server) propagate(args []string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	for ch := range s.replicas {
		select {
		case ch <- args:
		default:
			go func(c chan []string, a []string) {
				c <- a
			}(ch, args)
		}
	}
}

func (s *Server) startActiveExpiry() {
	ticker := time.NewTicker(100 * time.Millisecond)
	go func() {
		for range ticker.C {
			s.mu.Lock()
			isMaster := s.role == "master"
			s.mu.Unlock()
			if !isMaster {
				continue
			}

			var expiredKeys []string
			now := time.Now()

			for _, shard := range s.db.shards {
				shard.mu.Lock()
				count := 0
				for key, kv := range shard.db {
					if count > 20 {
						break
					}
					count++
					if !kv.ExpiresAt.IsZero() && kv.ExpiresAt.Before(now) {
						delete(shard.db, key)
						expiredKeys = append(expiredKeys, key)
					}
				}
				shard.mu.Unlock()
			}

			if len(expiredKeys) > 0 {
				s.propagate(append([]string{"DEL"}, expiredKeys...))
			}
		}
	}
}

func (s *Server) executeReplicaCommand(args []string) {
	if len(args) == 0 {
		return
	}
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "SET":
		if len(args) < 3 {
			return
		}
		key := args[1]
		val := args[2]
		var expiresAt time.Time
		for i := 3; i < len(args); i++ {
			opt := strings.ToUpper(args[i])
			if opt == "EX" && i+1 < len(args) {
				sec, _ := strconv.Atoi(args[i+1])
				expiresAt = time.Now().Add(time.Duration(sec) * time.Second)
				i++
			} else if opt == "PX" && i+1 < len(args) {
				msec, _ := strconv.Atoi(args[i+1])
				expiresAt = time.Now().Add(time.Duration(msec) * time.Millisecond)
				i++
			}
		}
		shard := s.db.getShard(key)
		shard.mu.Lock()
		shard.db[key] = KeyVal{Value: val, ExpiresAt: expiresAt}
		shard.mu.Unlock()

	case "DEL", "UNLINK":
		for _, key := range args[1:] {
			shard := s.db.getShard(key)
			shard.mu.Lock()
			delete(shard.db, key)
			shard.mu.Unlock()
		}

	case "EXPIRE":
		if len(args) < 3 {
			return
		}
		key := args[1]
		sec, _ := strconv.Atoi(args[2])
		shard := s.db.getShard(key)
		shard.mu.Lock()
		if kv, ok := shard.db[key]; ok {
			kv.ExpiresAt = time.Now().Add(time.Duration(sec) * time.Second)
			shard.db[key] = kv
		}
		shard.mu.Unlock()

	case "PEXPIREAT":
		if len(args) < 3 {
			return
		}
		key := args[1]
		msec, _ := strconv.ParseInt(args[2], 10, 64)
		shard := s.db.getShard(key)
		shard.mu.Lock()
		if kv, ok := shard.db[key]; ok {
			kv.ExpiresAt = time.Unix(0, msec*int64(time.Millisecond))
			shard.db[key] = kv
		}
		shard.mu.Unlock()
	}
}

func (s *Server) runReplica(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)

	// Handshake: PING
	_, err := conn.Write([]byte("*1\r\n$4\r\nPING\r\n"))
	if err != nil {
		return
	}
	_, err = reader.ReadString('\n')
	if err != nil {
		return
	}

	// Handshake: PSYNC
	_, err = conn.Write([]byte("*3\r\n$5\r\nPSYNC\r\n$1\r\n?\r\n$2\r\n-1\r\n"))
	if err != nil {
		return
	}
	_, err = reader.ReadString('\n') // +FULLRESYNC ...
	if err != nil {
		return
	}

	// Read RDB bulk string header
	resp, err := reader.ReadString('\n')
	if err != nil {
		return
	}
	var rdbLen int
	fmt.Sscanf(resp, "$%d\r\n", &rdbLen)
	rdbBuf := make([]byte, rdbLen+2)
	_, err = io.ReadFull(reader, rdbBuf)
	if err != nil {
		return
	}

	inbox := make(chan []string, 50000)
	s.mu.Lock()
	s.replicaInbox = inbox
	s.replicaConn = conn
	s.mu.Unlock()

	done := make(chan struct{})
	go func() {
		defer close(done)
		for args := range inbox {
			s.executeReplicaCommand(args)
		}
	}()

	for {
		args, err := readCommand(reader)
		if err != nil {
			break
		}
		if len(args) > 0 {
			inbox <- args
		}
	}

	close(inbox)
	<-done

	s.mu.Lock()
	if s.replicaConn == conn {
		s.replicaConn = nil
		s.replicaInbox = nil
	}
	s.mu.Unlock()
}

func (s *Server) promoteToMaster() {
	s.mu.Lock()
	if s.role == "master" {
		s.mu.Unlock()
		return
	}
	s.isPromoting = true
	conn := s.replicaConn
	s.mu.Unlock()

	if conn != nil {
		conn.Close()
	}

	// Wait until replicaInbox is nil, meaning the runReplica goroutine has finished draining and exited
	for {
		s.mu.Lock()
		inbox := s.replicaInbox
		s.mu.Unlock()
		if inbox == nil {
			break
		}
		time.Sleep(1 * time.Millisecond)
	}

	s.mu.Lock()
	s.role = "master"
	s.isPromoting = false
	s.mu.Unlock()
}

func (s *Server) startReplication(host, port string) {
	s.mu.Lock()
	if s.replicaConn != nil {
		s.replicaConn.Close()
	}
	s.role = "replica"
	s.masterHost = host
	s.masterPort = port
	s.mu.Unlock()

	conn, err := net.Dial("tcp", net.JoinHostPort(host, port))
	if err != nil {
		fmt.Printf("Failed to connect to master: %v\n", err)
		return
	}

	s.runReplica(conn)
}

func (s *Server) handlePSYNC(conn net.Conn) {
	conn.Write([]byte("+FULLRESYNC dummyrunid 0\r\n"))
	conn.Write([]byte("$0\r\n"))

	ch := make(chan []string, 50000)
	s.mu.Lock()
	s.replicas[ch] = true
	s.mu.Unlock()

	defer func() {
		s.mu.Lock()
		delete(s.replicas, ch)
		s.mu.Unlock()
	}()

	for _, shard := range s.db.shards {
		shard.mu.RLock()
		for key, kv := range shard.db {
			if kv.ExpiresAt.IsZero() || kv.ExpiresAt.After(time.Now()) {
				cmd := []string{"SET", key, kv.Value}
				if !kv.ExpiresAt.IsZero() {
					rem := time.Until(kv.ExpiresAt).Milliseconds()
					if rem > 0 {
						cmd = append(cmd, "PX", strconv.FormatInt(rem, 10))
					} else {
						continue
					}
				}
				conn.Write(formatRESP(cmd))
			}
		}
		shard.mu.RUnlock()
	}

	for args := range ch {
		_, err := conn.Write(formatRESP(args))
		if err != nil {
			return
		}
	}
}

func (s *Server) executeCommand(args []string, conn net.Conn) []byte {
	if len(args) == 0 {
		return nil
	}
	cmd := strings.ToUpper(args[0])
	switch cmd {
	case "PING":
		return []byte("+PONG\r\n")

	case "SET":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'set' command\r\n")
		}
		key := args[1]
		val := args[2]
		var expiresAt time.Time
		hasExpiry := false
		for i := 3; i < len(args); i++ {
			opt := strings.ToUpper(args[i])
			if opt == "EX" && i+1 < len(args) {
				sec, _ := strconv.Atoi(args[i+1])
				expiresAt = time.Now().Add(time.Duration(sec) * time.Second)
				hasExpiry = true
				i++
			} else if opt == "PX" && i+1 < len(args) {
				msec, _ := strconv.Atoi(args[i+1])
				expiresAt = time.Now().Add(time.Duration(msec) * time.Millisecond)
				hasExpiry = true
				i++
			}
		}

		shard := s.db.getShard(key)
		shard.mu.Lock()
		shard.db[key] = KeyVal{Value: val, ExpiresAt: expiresAt}
		shard.mu.Unlock()

		s.propagate([]string{"SET", key, val})
		if hasExpiry {
			msec := expiresAt.UnixNano() / int64(time.Millisecond)
			s.propagate([]string{"PEXPIREAT", key, strconv.FormatInt(msec, 10)})
		}
		return []byte("+OK\r\n")

	case "GET":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'get' command\r\n")
		}
		key := args[1]
		val, ok := s.get(key)
		if !ok {
			return []byte("$-1\r\n")
		}
		return []byte(fmt.Sprintf("$%d\r\n%s\r\n", len(val), val))

	case "DEL", "UNLINK":
		if len(args) < 2 {
			return []byte("-ERR wrong number of arguments for 'del' command\r\n")
		}
		count := 0
		for _, key := range args[1:] {
			shard := s.db.getShard(key)
			shard.mu.Lock()
			if _, ok := shard.db[key]; ok {
				delete(shard.db, key)
				count++
			}
			shard.mu.Unlock()
		}
		if count > 0 {
			s.propagate(args)
		}
		return []byte(fmt.Sprintf(":%d\r\n", count))

	case "EXPIRE":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'expire' command\r\n")
		}
		key := args[1]
		sec, err := strconv.Atoi(args[2])
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
		shard := s.db.getShard(key)
		shard.mu.Lock()
		success := 0
		if kv, ok := shard.db[key]; ok {
			expiresAt := time.Now().Add(time.Duration(sec) * time.Second)
			kv.ExpiresAt = expiresAt
			shard.db[key] = kv
			success = 1

			msec := expiresAt.UnixNano() / int64(time.Millisecond)
			s.propagate([]string{"PEXPIREAT", key, strconv.FormatInt(msec, 10)})
		}
		shard.mu.Unlock()
		return []byte(fmt.Sprintf(":%d\r\n", success))

	case "PEXPIREAT":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'pexpireat' command\r\n")
		}
		key := args[1]
		msec, err := strconv.ParseInt(args[2], 10, 64)
		if err != nil {
			return []byte("-ERR value is not an integer or out of range\r\n")
		}
		shard := s.db.getShard(key)
		shard.mu.Lock()
		success := 0
		if kv, ok := shard.db[key]; ok {
			kv.ExpiresAt = time.Unix(0, msec*int64(time.Millisecond))
			shard.db[key] = kv
			success = 1
		}
		shard.mu.Unlock()
		if success == 1 {
			s.propagate(args)
		}
		return []byte(fmt.Sprintf(":%d\r\n", success))

	case "REPLICAOF":
		if len(args) < 3 {
			return []byte("-ERR wrong number of arguments for 'replicaof' command\r\n")
		}
		host := args[1]
		port := args[2]
		if strings.ToUpper(host) == "NO" && strings.ToUpper(port) == "ONE" {
			s.promoteToMaster()
			return []byte("+OK\r\n")
		} else {
			go s.startReplication(host, port)
			return []byte("+OK\r\n")
		}

	case "PSYNC":
		go s.handlePSYNC(conn)
		return nil

	default:
		return []byte(fmt.Sprintf("-ERR unknown command '%s'\r\n", cmd))
	}
}

func (s *Server) handleClient(conn net.Conn) {
	defer conn.Close()
	reader := bufio.NewReader(conn)
	for {
		args, err := readCommand(reader)
		if err != nil {
			return
		}
		if len(args) == 0 {
			continue
		}

		cmd := strings.ToUpper(args[0])
		if cmd != "REPLICAOF" {
			for {
				s.mu.Lock()
				promoting := s.isPromoting
				s.mu.Unlock()
				if !promoting {
					break
				}
				time.Sleep(1 * time.Millisecond)
			}
		}

		resp := s.executeCommand(args, conn)
		if resp != nil {
			_, err = conn.Write(resp)
			if err != nil {
				return
			}
		}
	}
}

func (s *Server) Start() {
	s.startActiveExpiry()

	listener, err := net.Listen("tcp", ":"+s.port)
	if err != nil {
		fmt.Printf("Failed to listen on port %s: %v\n", s.port, err)
		return
	}
	defer listener.Close()
	fmt.Printf("Server listening on port %s...\n", s.port)

	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Printf("Failed to accept connection: %v\n", err)
			continue
		}
		go s.handleClient(conn)
	}
}

func main() {
	port := flag.String("port", "6379", "Port to listen on")
	flag.Parse()

	server := NewServer(*port)
	server.Start()
}
