package leader

import (
	"errors"
	"fmt"
	zk "github.com/samuel/go-zookeeper/zk"
	"sort"
	"strconv"
	"strings"
	"time"
)

// Candidate represents a Election client that has requested leadership. It consists of a CandidateID
// and a LeaderNotificationChnl. CandidateID uniquely identifies a specific client that has requested leadership
// for a resource. LeaderNotificationChnl is used by the library to notify a candidate that was not initially
// elected leader that it has assumed the leadership role for the resource.
type Candidate struct {
	CandidateID            string
	LeaderNotificationChnl <-chan string // TODO: DELETE
}

// Election is a structure that represents a new instance of a Election. This instance can then
// be used to request leadership for a specific resource.
type Election struct {
	electionNode  string
	candidate     Candidate
	isLeader      bool
	zkConn        *zk.Conn
	ldrshpChgChnl chan bool
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

	return Election{electionNode, Candidate{}, false, zkConn, nil}, nil
}

// ElectLeader will, for a given nomineePrefix and resource, make the caller a candidate
// for leadership and determine if the candidate becomes the leader.
// The parameters are:
//     nomineePrefix - a generic prefix (e.g., n_) for the election and a resource for which
//     the election is being held (e.g., president).
// It returns true if leader and a string representing the full path to the candidate ID
// (e.g., /election/president/n_00001). The candidate ID is needed when and if a candidate
// wants to resign as leader.
func (le *Election) ElectLeader(nomineePrefix, resource string) (bool, Candidate, chan bool) {
	candidate := makeOffer(nomineePrefix, le)
	le.candidate = candidate
	isLeader := determineLeader(candidate.CandidateID, le)
	//	fmt.Println("Election Result: Leader?", isLeader, "; Candidate info:", le.candidate.CandidateID)

	if !isLeader {
		le.ldrshpChgChnl = make(chan bool, 10)
	}

	return isLeader, candidate, le.ldrshpChgChnl
}

// IsLeader returns true if the provided id is the leader, false otherwise.
// Parameters:
//	id - The ID of the candidate to be tested for leadership.
func (le *Election) IsLeader() bool {
	return le.isLeader
}

func (le *Election) ID() string {
	return le.candidate.CandidateID
}
//	candidate - The candidate who is resigning. The value for candidate is returned from ElectLeader.
func (le *Election) Resign(candidate Candidate) error {
	if le.IsLeader() {
		le.isLeader = false
	}
	fmt.Println("\t\tResign:", candidate.CandidateID)
	err := le.zkConn.Delete(candidate.CandidateID, -1)
	if err != nil {
		fmt.Println("Candidate", le.candidate.CandidateID, "failed to resign because of", err, ".  Should retry here")
		time.Sleep(5 * time.Second)
		err = le.zkConn.Delete(candidate.CandidateID, -1)
		if err != nil {
			// At this point reconnection attempts may not succeed. Continue with resignation process.
			// The larger application should handle persistent connection problems by restarting
			fmt.Println("Candidate", le.candidate.CandidateID, "failed to resign because of", err, 
				".  Retried once, panicking")
			err2 := fmt.Errorf("Unable to Resign due to error (%v)", err)
			return err2
		}
	}
	le.candidate = Candidate{}
	return nil
}

// String is the Stringer implementation for this type.
func (le Election) String() string {
	connected := "no"
	if le.zkConn != nil {
		connected = "yes"
	}
	return "Election:" +
		"\n\telectionNode: \t" + le.electionNode +
		"\n\tleader: \t" + strconv.FormatBool(le.isLeader) +
		"\n\tconnected?: \t" + connected
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
	return candidate
}

func determineLeader(candidateID string, le *Election) bool {
	//	fmt.Println("determineLeader: path", path)
	// TODO: DELETE
	//	candidates, _, evtChl, _ := le.zkConn.ChildrenW(le.electionNode)
	// TODO: How to make this more bullet-proof? Check stat, err? Must have at least
	// TODO: one child, me?
	candidates, _, _ := le.zkConn.Children(le.electionNode)
	if len(candidates) == 0 {
		fmt.Println("No children exist in ZK, not even me", candidateID)
		panic(errors.New("No children exist in ZK"))
	}

	sort.Strings(candidates)
	//	fmt.Println("Sorted Leader nominee list:", candidates)
	pathNodes := strings.Split(candidateID, "/")
	lenPath := len(pathNodes)
	//	fmt.Println("Path nodes:", pathNodes, "len:", lenPath)
	shortCndtID := pathNodes[lenPath-1]
	//	fmt.Println("Election ID:", shortCndtID)
	if strings.EqualFold(shortCndtID, candidates[0]) {
		le.isLeader = true
		return le.isLeader
	}

	// Not leader, so watch next highest candidate
	idx := sort.SearchStrings(candidates, shortCndtID)
	var nodeToWatchIdx int
	if strings.EqualFold(candidates[idx], shortCndtID) {
		nodeToWatchIdx = idx - 1
	} else {
		// TODO: WTF, this shouldn't happen. How to best handle this?
		fmt.Println("candidates[0]:", candidates[0])
		fmt.Println("candidates[1]:", candidates[1])
		fmt.Println("candidate", candidateID, "doesn't exist in ZK")
		panic(errors.New("Candidate doesn't exist in ZK"))
	}
	// TODO: How to make this more bullet-proof? If doesn't exist
	// TODO: should it go through the remaining prior Candidates? That will happen if
	// TODO: this node is watching an intermediate, non-leader, node that fails.
	// TODO: What if none of them exist (i.e., len(candidates) == 0, but then it would be leader), then it should become leader.
	watchedNode := strings.Join([]string{le.electionNode, candidates[nodeToWatchIdx]}, "/")
	exists, _, watchChl, _ := le.zkConn.ExistsW(watchedNode)
	if !exists {
		fmt.Println("Watched candidate", watchedNode, "doesn't exist in ZK")
		panic(errors.New("Watched candidate doesn't exist in ZK"))
	}
	//	fmt.Println("Candidate:", candidateID, "is watching candidate:", watchedNode)
	//	fmt.Println("Does", watchedNode, "exist?", exists)
	//	fmt.Println("Candidate", candidateID, "will watch on channel", watchChl)
	go watchForLeadershipChanges(watchChl, le, candidateID)

	return false
}

func watchForLeadershipChanges(watchChnl <-chan zk.Event, le *Election, candidateID string) {
	watchEvt := <-watchChnl
	//	fmt.Println("\tdetermineLeader.go func(), watchChnl event fired:", watchEvt,
	//		"for Candidate", candidateID)
	if watchEvt.Type == zk.EventNodeDeleted {
		//	children, _, _, _ := le.zkConn.ChildrenW(le.electionNode)
		//	fmt.Println("\tdetermineLeader.go func() - Remaining Children:")
		//	fmt.Println("\t\t", children)
		le.ldrshpChgChnl <- determineLeader(candidateID, le)
		//	fmt.Println("\tDone with leader re-election", candidateID)

	}
	// TODO: change this to watching just the previous child vs. all children
	le.zkConn.ChildrenW(le.electionNode)
}
