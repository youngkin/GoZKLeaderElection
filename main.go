package main

import (
	"fmt"
	zk "github.com/samuel/go-zookeeper/zk"
	"github.com/youngkin/GoZKLeaderElection/leader"
	"sync"
	"strings"
	"time"
)

//Tests:
//	1. Of multiple candidates, only 1 becomes leader
//	2. When a leader of multiple candidates resigns, one of the remaining candidates is chosen as leader
//	3. When the last leader resigns there is no leader and no remaining candidates
//	4. When the ZK connection is lost all candidates are notified and WHAT HAPPENS???
//	5. How does it work in a distributed (i.e., multi-process/multi-host) environment
//

type ElectionResponse struct {
	IsLeader    bool
	CandidateID string
}

func main() {
	respCh := make(chan ElectionResponse)
	conn, _ := connect("192.168.12.11:2181")
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go runCandidate(conn, "/election", &wg, respCh, uint(i))
	}

	go func() {
		wg.Wait()
		close(respCh)
	}()

	responses := make([]ElectionResponse, 0)
	for response := range respCh {
		fmt.Println("Final Election result:", response)
	}

	//	fmt.Println("\n\nCandidates at end:", le.String())
	verifyResults(responses)
}

func connect(zksStr string) (*zk.Conn, <-chan zk.Event) {
	//	zksStr := os.Getenv("ZOOKEEPER_SERVERS")
	zks := strings.Split(zksStr, ",")
	conn, evtChnl, err := zk.Connect(zks, time.Second)
	must(err)
	return conn, evtChnl
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func runCandidate(zkConn *zk.Conn, electionPath string, wg *sync.WaitGroup, respCh chan ElectionResponse, waitFor uint) {
	leaderElector, err := leader.NewElection(zkConn, "/election")
	must(err)
	fmt.Println(leaderElector.String(), "\n\n")

	isLeader, candidate := leaderElector.ElectLeader("n_", "president")
	//	fmt.Println("leaderElector AFTER ELECTION: leaderElector.IsLeader(", id, ")?:", leaderElector.IsLeader(id))

	sleepMillis := (waitFor*waitFor + 1) * 100
	time.Sleep(time.Duration(sleepMillis) * time.Millisecond)
	leaderElector.Resign(candidate)
	//	fmt.Println("leaderElector AFTER RESIGN: leaderElector.IsLeader()?:", leaderElector.IsLeader(candidate.CandidateID))
	time.Sleep(time.Duration(sleepMillis) * time.Millisecond)
	respCh <- ElectionResponse{isLeader, candidate.CandidateID}
	wg.Done()
}

func verifyResults(responses []ElectionResponse) {
	onlyOneLeader := false
	testPassed := true
	for _, leaderResp := range responses {
		if leaderResp.IsLeader == true {
			if onlyOneLeader {
				testPassed = false
				break
			}
			onlyOneLeader = true
		}
	}
	if testPassed {
		fmt.Println("\nTEST PASSED, HOORAY!!!")
	} else {
		fmt.Println("\nTEST FAILED, multiple leaders detected")
	}
}
