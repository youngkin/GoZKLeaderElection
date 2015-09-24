package leader

import (
	"fmt"
	"github.com/samuel/go-zookeeper/zk"
	"strings"
	"time"
	"sort"
)

type LeaderElector struct {
	zkHost       string
	electionNode string
	isLeader     bool
	zkConn       *zk.Conn
	candidates []string
}

func NewLeaderElector(zkAddr string, electionNode string) (LeaderElector, error) {
	conn := connect(zkAddr)
	//TODO: what should flags and acl be set to?
	flags := int32(0)
	acl := zk.WorldACL(zk.PermAll)

	exists, _, _ := conn.Exists(electionNode)
	var (
		path string
		err error
		)
	if !exists {
		path, err = conn.Create(electionNode, []byte("data"), flags, acl)
		must(err)
		fmt.Printf("created: %+v\n", path)
	}
	
	candidates := make([]string, 5)
	return LeaderElector{zkAddr, electionNode, false, conn, candidates}, nil
}

func (le *LeaderElector) Connection() *zk.Conn {
	return le.zkConn
}

func (le *LeaderElector) IsLeader() bool {
	return le.isLeader
}

func (le *LeaderElector) NominateAndElect(nomineePrefix, resource string) bool {
	path := makeOffer(nomineePrefix, *le)
	le.isLeader = determineLeader(path, le)
	return le.isLeader
}

func (le *LeaderElector) Resign() {
	le.isLeader = false
	le.zkConn.Close()
	return
}

func (le LeaderElector) String() string {
	connected := "no"
	if le.zkConn != nil {
		connected = "yes"
	}
	amILeader := "no"
	if le.isLeader {
		amILeader = "yes"
	}
	return "LeaderElector:" +
		"\n\tzkHost: \t" + le.zkHost +
		"\n\telectionNode: \t" + le.electionNode +
		"\n\tisLeader: \t" + amILeader +
		"\n\tconnected?: \t" + connected
}

func main() {
	leaderElector, err := NewLeaderElector("192.168.12.11:2181", "/election")
	must(err)
	fmt.Println(leaderElector.String())
	fmt.Println("leaderElector BEFORE ELECTION: leaderElector.IsLeader()?:", leaderElector.IsLeader())
	isLeader := leaderElector.NominateAndElect("n_", "president")
	fmt.Println("leaderElector AFTER ELECTION: isLeader?:", isLeader)
	fmt.Println("leaderElector AFTER ELECTION: leaderElector.IsLeader()?:", leaderElector.IsLeader())

	// start more candidates to see behavior when multiple candidates vie for leader
	le2, err2 := NewLeaderElector("192.168.12.11:2181", "/election") //add another candidate
	must(err2)
	le3, err3 := NewLeaderElector("192.168.12.11:2181", "/election") //add yet another candidate
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

func connect(zksStr string) *zk.Conn {
//	zksStr := os.Getenv("ZOOKEEPER_SERVERS")
	zks := strings.Split(zksStr, ",")
	conn, _, err := zk.Connect(zks, time.Second)
	must(err)
	return conn
}

func makeOffer(nomineePrefix string, le LeaderElector) string {
	flags := int32(zk.FlagSequence | zk.FlagEphemeral)
	acl := zk.WorldACL(zk.PermAll)

	// Make offer
	path, err := le.zkConn.Create(strings.Join([]string{le.electionNode, nomineePrefix}, "/"), []byte("here"), flags, acl)
	must(err)
	fmt.Printf("created: %+v\n", path)
	return path	
}

func determineLeader(path string, le *LeaderElector) bool {
	fmt.Println("determineLeader: path", path)
	snapshot, _, _, _ := le.zkConn.ChildrenW(le.electionNode)
	sort.Strings(snapshot)
	fmt.Println("Sorted Leader nominee list:", snapshot)
	pathNodes := strings.Split(path, "/")
	lenPath := len(pathNodes)
	fmt.Println("Path nodes:", pathNodes, "len:", lenPath)
	myID := pathNodes[lenPath - 1] 
	fmt.Println("Election ID:", myID)
	if strings.EqualFold(myID, snapshot[0]) {
		le.isLeader = true 
	} else {
		le.isLeader = false
	}
	return le.isLeader
}
