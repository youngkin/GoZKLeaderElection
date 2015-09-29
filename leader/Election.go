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
	electionNode string
	candidate    Candidate
	// TODO: leader can be changed to a bool. See IsLeader() for more details.
	leader       string
	zkConn       *zk.Conn
	// TODO: Probably don't need candidates. It's currently not set to anything except the single
	// TODO: candidate associated with an instance of Election.
	candidates   []Candidate
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
func NewElection(zkConn *zk.Conn, electionNode string) (Election, error) {
	//TODO: what should flags and acl be set to?
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)

	exists, _, _ := zkConn.Exists(electionNode)
	var (
		path string
		err  error
	)
	if !exists {
		path, err = zkConn.Create(electionNode, []byte("data"), flags, acl)
		must(err)
		fmt.Printf("created: %+v\n", path)
	}

	var candidates []Candidate
	return Election{electionNode, Candidate{}, "", zkConn, candidates}, nil
}

// IsLeader returns true if the provided id is the leader, false otherwise.
// Parameters:
//	id - The ID of the candidate to be tested for leadership.
func (le *Election) IsLeader(id string) bool {
	// TODO: Since an instance of Election is unique to a single candidate all that's
	// TODO: needed is a boolean that's set during leader election that indicates
	// TODO: whether this Election instance represents the leader. So no parm is
	// TODO: needed and this function is simply a getter for the isLeader field.
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
	le.candidate = candidate
	fmt.Println("Election Result: Leader?", isLeader, "; Candidate info:", le.candidate.CandidateID)
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
	fmt.Println("Resign:", candidate.CandidateID)
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
	candidates, _, evtChl, _ := le.zkConn.ChildrenW(le.electionNode)

	// Watch for events on the children
	go func(watchChnl <-chan zk.Event) {
		fmt.Println("determineLeader.go func(), watchChnl event fired:", <-watchChnl)
		children, _, _, _ := le.zkConn.ChildrenW(le.electionNode)
		fmt.Println("determineLeader.go func() - Remaining Children:")
		fmt.Println("\t", children)
		time.Sleep(10 * time.Millisecond)
	}(evtChl)

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
