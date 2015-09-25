package main

import (
	"fmt"
	"github.com/youngkin/GoZKLeaderElection/leader"
	"sync"
)

//Tests:
//	1. Of multiple candidates, only 1 becomes leader
//	2. When a leader of multiple candidates resigns, one of the remaining candidates is chosen as leader
//	3. When the last leader resigns there is no leader and no remaining candidates
//	4. When the ZK connection is lost all candidates are notified and WHAT HAPPENS???

type ElectionResponse struct {
	IsLeader bool
	CandidateID string
}
func main() {
	le, err := leader.NewLeaderElector("192.168.12.11:2181", "/election")
	must(err)
	fmt.Println(le.String(), "\n\n")

	respCh := make(chan ElectionResponse)
	var wg sync.WaitGroup
	for i := 0; i < 3; i++ {
		wg.Add(1)
		go runCandidate("192.168.12.11:2181", "/election", &wg, &le, respCh)
	}
	
	go func() {
		wg.Wait()
		close(respCh)
	}()
	
	responses := make([]ElectionResponse, 0)
	for response := range respCh {
		fmt.Println("Election result:", response)
	}
	
	fmt.Println("\n\nCandidates at end:", le.String())
	verifyResults(responses)
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}

func runCandidate(zkHost, electionPath string, wg *sync.WaitGroup, leaderElector *leader.LeaderElector, respCh chan ElectionResponse) {
	isLeader, candidate := leaderElector.ElectLeader("n_", "president")
//	fmt.Println("leaderElector AFTER ELECTION: leaderElector.IsLeader(", id, ")?:", leaderElector.IsLeader(id))

	leaderElector.Resign(candidate)
//	fmt.Println("leaderElector AFTER RESIGN: leaderElector.IsLeader()?:", leaderElector.IsLeader(id))
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
