package leader

import (
	"fmt"
	zk "github.com/samuel/go-zookeeper/zk"
	"sort"
	"strings"
	"time"
)

// Candidate represents a Election client that has requested leadership. It consists of a CandidateID
// and a LeaderNotificationChnl. CandidateID uniquely identifies a specific client that has requested leadership
// for a resource. LeaderNotificationChnl is used by the library to notify a candidate that was not initially
// elected leader that it has assumed the leadership role for the resource.
type Candidate struct {
	CandidateID            string
	LeaderNotificationChnl <-chan string
}

// Election is a structure that represents a new instance of a Election. This instance can then
// be used to request leadership for a specific resource.
type Election struct {
	zkHost       string
	electionNode string
	leader       string
	zkConn       *zk.Conn
	candidates   []Candidate
	zkEventChnl  <-chan zk.Event
}

// NewElection initializes a new instance of a Election that can later be used to request
// leadership for a specific resource.
//
// It accepts:
//	zkConn - a connection to a running Zookeeper instance
//	resource - the resource for which leadership is being requested
//
// It will return either a non-nil Election instance and a nil error, or a nil
// Election and a non-nil error.
//
func NewElection(zkAddr string, electionNode string) (Election, error) {
	conn, evtChnl := connect(zkAddr)
	//TODO: what should flags and acl be set to?
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)

	exists, _, _ := conn.Exists(electionNode)
	var (
		path string
		err  error
	)
	if !exists {
		path, err = conn.Create(electionNode, []byte("data"), flags, acl)
		must(err)
		fmt.Printf("created: %+v\n", path)
	}

	var candidates []Candidate
	return Election{zkAddr, electionNode, "", conn, candidates, evtChnl}, nil
}

// IsLeader returns true if the provided id is the leader, false otherwise.
// Parameters:
//	id - The ID of the candidate to be tested for leadership.
func (le *Election) IsLeader(id string) bool {
	return strings.EqualFold(le.leader, id)
}

// ElectLeader will, for a given nomineePrefix and resource, make the caller a candidate
// for leadership and determine if the candidate becomes the leader.
// The parameters are:
//     nomineePrefix - a generic prefix (e.g., n_) for the election and a resource for which
//     the election is being held (e.g., president).
// It returns true if leader and a string representing the full path to the candidate ID
// (e.g., /election/president/n_00001). The candidate ID is needed when and if a candidate
// wants to resign as leader.
func (le *Election) ElectLeader(nomineePrefix, resource string) (bool, Candidate) {
	candidate := makeOffer(nomineePrefix, le)
	isLeader := determineLeader(candidate.CandidateID, le)
	return isLeader, candidate
}

// ElectAndSucceedLeader will, for a given nomineePrefix and resource, make the caller a candidate
// for leadership and determine if the candidate becomes the leader.
// The parameters are:
//     nomineePrefix - a generic prefix (e.g., n_) for the election and a resource for which
//     the election is being held (e.g., president).
// It returns true if leader, a channel to notify candidates that the previous leader failed/resigned
// and now they are the leader, and a string representing the full path to the candidate ID
// (e.g., /election/president/n_00001). The candidate ID is needed when and if a candidate
// wants to resign as leader.
func (le *Election) ElectAndSucceedLeader(nomineePrefix, resource string) (bool, Candidate) {
	candidate := makeOffer(nomineePrefix, le)
	isLeader := determineLeader(candidate.CandidateID, le)
	if !isLeader {
		monitorLeaderChange(candidate.CandidateID, le)
	}
	return isLeader, candidate
}

// Resign removes the associated Election as leader or follower for the associated resource.
//	candidate - The candidate who is resigning. The value for candidate is returned from ElectLeader.
func (le *Election) Resign(candidate Candidate) {
	if strings.EqualFold(le.leader, candidate.CandidateID) {
		le.leader = ""
	}
	le.zkConn.Delete(candidate.CandidateID, -1)
	removeCandidate(le, candidate.CandidateID)
	return
}

// String is the Stringer implementation for this type.
func (le Election) String() string {
	connected := "no"
	if le.zkConn != nil {
		connected = "yes"
	}
	var candidatesAsString string
	for _, candidate := range le.candidates {
		candidatesAsString = candidatesAsString + candidate.CandidateID + " "
	}
	return "Election:" +
		"\n\tzkHost: \t" + le.zkHost +
		"\n\telectionNode: \t" + le.electionNode +
		"\n\tleader: \t" + le.leader +
		"\n\tconnected?: \t" + connected +
		"\n\tcandidates: \t" + candidatesAsString
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func connect(zksStr string) (*zk.Conn, <-chan zk.Event) {
	//	zksStr := os.Getenv("ZOOKEEPER_SERVERS")
	zks := strings.Split(zksStr, ",")
	conn, evtChnl, err := zk.Connect(zks, time.Second)
	must(err)
	return conn, evtChnl
}

func makeOffer(nomineePrefix string, le *Election) Candidate {
	flags := int32(zk.FlagSequence | zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)

	// Make offer
	path, err := le.zkConn.Create(strings.Join([]string{le.electionNode, nomineePrefix}, "/"), []byte("here"), flags, acl)
	must(err)
	leaderNotificationChnl := make(chan string)
	//	fmt.Printf("makeOffer: created: %+v\n", path)
	candidate := Candidate{path, leaderNotificationChnl}
	le.candidates = append(le.candidates, candidate)
	//	fmt.Println("makeOffer: candidates:", le.candidates)
	return candidate
}

func monitorLeaderChange(candidateID string, le *Election) <-chan string {
	ldrshpChgChan := make(<-chan string)
	return ldrshpChgChan
}

func determineLeader(path string, le *Election) bool {
	//	fmt.Println("determineLeader: path", path)
	candidates, _, _, _ := le.zkConn.ChildrenW(le.electionNode)
	sort.Strings(candidates)
	//	fmt.Println("Sorted Leader nominee list:", candidates)
	pathNodes := strings.Split(path, "/")
	lenPath := len(pathNodes)
	//	fmt.Println("Path nodes:", pathNodes, "len:", lenPath)
	myID := pathNodes[lenPath-1]
	//	fmt.Println("Election ID:", myID)
	if strings.EqualFold(myID, candidates[0]) {
		le.leader = path
		return true
	}

	return false
}

func removeCandidate(le *Election, candidateID string) {
	for i, candidate := range le.candidates {
		if strings.EqualFold(candidate.CandidateID, candidateID) {
			le.candidates = append(le.candidates[:i], le.candidates[i+1:]...)
			break
		}
	}
}
