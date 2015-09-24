package main

import (
	"fmt"
	"github.com/youngkin/GoZKLeaderElection"
)

func main() {
	leaderElector, err := LeaderElector.NewLeaderElector("192.168.12.11:2181", "/election")
	must(err)
	fmt.Println(leaderElector.String())
	fmt.Println("leaderElector BEFORE ELECTION: leaderElector.IsLeader()?:", leaderElector.IsLeader())
	isLeader := leaderElector.NominateAndElect("n_", "president")
	fmt.Println("leaderElector AFTER ELECTION: isLeader?:", isLeader)
	fmt.Println("leaderElector AFTER ELECTION: leaderElector.IsLeader()?:", leaderElector.IsLeader())

	// start more candidates to see behavior when multiple candidates vie for leader
	le2, err2 := LeaderElector.NewLeaderElector("192.168.12.11:2181", "/election") //add another candidate
	must(err2)
	le3, err3 := LeaderElector.NewLeaderElector("192.168.12.11:2181", "/election") //add yet another candidate
	must(err3)

	fmt.Println("\n")
	le2Leader := le2.NominateAndElect("n_", "president")
	fmt.Println("leaderElector2 AFTER ELECTION: isLeader?:", le2Leader)

	fmt.Println("\n")
	le3Leader := le3.NominateAndElect("n_", "president")
	fmt.Println("leaderElector3 AFTER ELECTION: isLeader?:", le3Leader)
	
	fmt.Println("\n")
	leaderElector.Resign()
	fmt.Println("leaderElector AFTER RESIGN: leaderElector.IsLeader()?:", leaderElector.IsLeader())
}

func must(err error) {
	if err != nil {
		panic(err)
	}
}
