package main

import (
	"os"
	"os/signal"
	"syscall"
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"path"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/guo-shaoge/supervisorProto/google.golang.org/grpc/examples/supervisor/supervisor"
	etcdcliv3 "go.etcd.io/etcd/clientv3"
	etcdembed "go.etcd.io/etcd/embed"
	_ "go.uber.org/zap"
	"google.golang.org/grpc"
)

// HTTP Server:
//   1. start tiflash_compute node. Input: POD_IP, tenant_name.
//   2. stop tiflash_compute node: Input: POD_IP, tenant_name.
//   3. get all tiflash_compute nodes.

// Etcd Server:
//   1. publish new topo when start/stop tiflash_compute node.

const (
	// NOTE: maybe different path. Talk with @woody.
	etcdAddrsPath      = "/tiflash_compute_topo/addrs"
	etcdTenantsPath    = "/tiflash_compute_topo/tenants"
	supervisorPort     = "7000"
	tiflashServicePort = "3930"
	topoSep            = ";"

	// If change this, also need to change Dockerfile.
	mockAutoScalerPort = "8888"
)

type computeTopo struct {
	addrs   []string
	tenants []string
}

var (
	etcdCli     *etcdcliv3.Client
	errLogger   *log.Logger
	infoLogger  *log.Logger
	debugLogger *log.Logger
	globalTopo  *computeTopo
	// TODO: maybe better performance.
	mu sync.Mutex
)

func initLogger(basePath string) {
	w, err := os.OpenFile(path.Join(basePath, "log"), os.O_WRONLY|os.O_CREATE, 0755)
	if err != nil {
		log.Fatalf("create file log.txt failed: %v", err)
	}

	errLogger = log.New(w, "[ERROR]", log.Ldate|log.Ltime|log.Lmicroseconds)
	infoLogger = log.New(w, "[INFO]", log.Ldate|log.Ltime|log.Lmicroseconds)
	debugLogger = log.New(w, "[DEBUG]", log.Ldate|log.Ltime|log.Lmicroseconds)
}

func parseRequest(r *http.Request) (string, string, error) {
	// Get assign info: podIP and tenantName.
	podIPs, ok := r.URL.Query()["pod_ip"]
	if !ok {
		return "", "", fmt.Errorf("cannot find pod_ip parameter in url. url: %s", r.URL.String())
	}
	if len(podIPs) != 1 {
		return "", "", fmt.Errorf("length of podIP in url is not 1. len: %d, url: %s", len(podIPs), r.URL.String())
	}
	podIP := podIPs[0]
	tenantNames, ok := r.URL.Query()["tenant_name"]
	if !ok {
		return "", "", fmt.Errorf("cannot find tenant_name parameter in url. url: %s", r.URL.String())
	}
	if len(tenantNames) != 1 {
		return "", "", fmt.Errorf("length of tenantNames in url is not 1. len: %d, url: %s", len(podIPs), r.URL.String())
	}
	tenantName := tenantNames[0]

	return podIP, tenantName, nil
}

func writeError(w http.ResponseWriter, resStatus int, errMsg string) {
	errLogger.Print(errMsg)
	w.WriteHeader(resStatus)
	w.Write([]byte(errMsg))
}

func getEtcdTopo(path string) (fullTopo string, errMsg string) {
	etcdGetResp, err := etcdcliv3.NewKV(etcdCli).Get(context.TODO(), path)
	if err != nil {
		errMsg = fmt.Sprintf("get etcd failed. err: %s", err.Error())
		return
	}
	if len(etcdGetResp.Kvs) > 1 {
		errMsg = fmt.Sprintf("unexpected etcd kv length, got: %d", len(etcdGetResp.Kvs))
		return
	} else if len(etcdGetResp.Kvs) == 0 {
		infoLogger.Printf("etcd kv length is 0")
	} else {
		fullTopo = string(etcdGetResp.Kvs[0].Value)
		infoLogger.Printf("etcd is not empty, ori is: %s", fullTopo)
	}
	return
}

func putEtcdTopo(val, path string) (errMsg string) {
	etcdPutResp, err := etcdcliv3.NewKV(etcdCli).Put(context.TODO(), path, val)
	if err != nil {
		errMsg = fmt.Sprintf("put etcd failed. err: %s", err.Error())
		return
	}
	infoLogger.Printf("put etcd succeed. val: %s, path: %s, resp: %v", val, path, etcdPutResp)
	return
}

func splitAndDelete(val, del string) (newVal string, errMsg string) {
	topoList := strings.Split(val, topoSep)
	var deleted bool
	for _, t := range topoList {
		if t == del {
			deleted = true
			continue
		}
		if len(newVal) != 0 {
			newVal += topoSep
		}
		newVal += t
	}
	if !deleted {
		errMsg = fmt.Sprintf("splitAndDelete failed, val: %s, del: %s", val, del)
		return
	}
	return
}

func httpScaleOut(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	infoLogger.Printf("[http] got ScaleOut http request: %s", r.URL.String())

	podIP, tenantName, err := parseRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Notify supervisor to start tiflash.
	grpcAddr := podIP + ":" + supervisorPort
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	if err != nil {
		errMsg := fmt.Sprintf("dial grpc failed. addr: %s, err: %v", grpcAddr, err)
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	if conn != nil {
		defer conn.Close()
	}

	c := supervisor.NewAssignClient(conn)

	mockAddr := "1.1.1.1:1111"
	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	grpcResp, err :=
		c.AssignTenant(
			ctx,
			&supervisor.AssignRequest{TenantID: tenantName, TidbStatusAddr: mockAddr, PdAddr: mockAddr})
	if err != nil {
		errMsg := fmt.Sprintf("call AssignTenant grpc failed. addr: %s, err: %v", grpcAddr, err)
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	if grpcResp.HasErr {
		errMsg := fmt.Sprintf("call AssignTenant grpc return error. err: %s", grpcResp.ErrInfo)
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}

	// Update etcd topo.
	fullTopo, errMsg := getEtcdTopo(etcdAddrsPath)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	fullTenants, errMsg := getEtcdTopo(etcdTenantsPath)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}

	addedTiFlashAddr := podIP + ":" + tiflashServicePort
	if len(fullTopo) == 0 {
		fullTopo = addedTiFlashAddr
		fullTenants = tenantName
		infoLogger.Printf("etcd is empty, will add %s", addedTiFlashAddr)
	} else {
		infoLogger.Printf("etcd is not empty, ori is: %s, will add %s", fullTopo, addedTiFlashAddr)
		fullTopo += topoSep + addedTiFlashAddr
		fullTenants += topoSep + tenantName
	}

	// TODO: Txn
	errMsg = putEtcdTopo(etcdAddrsPath, fullTopo)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	errMsg = putEtcdTopo(etcdTenantsPath, fullTenants)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

func httpScaleIn(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	infoLogger.Printf("[http] got ScaleIn http request: %s", r.URL.String())

	podIP, tenantName, err := parseRequest(r)
	if err != nil {
		writeError(w, http.StatusBadRequest, err.Error())
		return
	}

	// Notify supervisor to start tiflash.
	grpcAddr := podIP + ":" + supervisorPort
	conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
	if err != nil {
		errMsg := fmt.Sprintf("dial grpc failed. addr: %s, err: %v", grpcAddr, err)
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	if conn != nil {
		defer conn.Close()
	}

	c := supervisor.NewAssignClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
	defer cancel()

	grpcResp, err :=
		c.UnassignTenant(
			ctx,
			&supervisor.UnassignRequest{AssertTenantID: tenantName})
	if err != nil {
		errMsg := fmt.Sprintf("call UnassignTenant grpc failed. addr: %s, err: %v", grpcAddr, err)
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	if grpcResp.HasErr {
		errMsg := fmt.Sprintf("call UnassignTenant grpc return error. err: %s", grpcResp.ErrInfo)
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}

	// Update etcd topo.
	var fullTopo string

	fullTopo, errMsg := getEtcdTopo(etcdAddrsPath)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	fullTenants, errMsg := getEtcdTopo(etcdTenantsPath)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}

	var newFullTopo string
	var newFullTenants string
	delTenant := tenantName
	delAddr := podIP + ":" + supervisorPort
	if len(fullTopo) == 0 {
		infoLogger.Printf("etcd is empty, skip scale-in %s, %s", delAddr, delTenant)
	} else {
		infoLogger.Printf("etcd is not empty, ori is: %s, will scale-in %s, %s", fullTopo, delAddr, delTenant)
		newFullTopo, errMsg = splitAndDelete(fullTopo, delAddr)
		if len(errMsg) != 0 {
			writeError(w, http.StatusInternalServerError, errMsg)
			return
		}
		newFullTenants, errMsg = splitAndDelete(fullTenants, delTenant)
		if len(errMsg) != 0 {
			writeError(w, http.StatusInternalServerError, errMsg)
			return
		}
	}

	errMsg = putEtcdTopo(etcdAddrsPath, newFullTopo)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	errMsg = putEtcdTopo(etcdTenantsPath, newFullTenants)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}

	w.WriteHeader(http.StatusOK)
	return
}

func httpFetchTopo(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	infoLogger.Printf("[http] got FetchTopo http request: %s", r.URL.String())

	fullTopo, errMsg := getEtcdTopo(etcdAddrsPath)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	infoLogger.Printf("[http] FetchTopo done: %s", fullTopo)

	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fullTopo))
	return
}

// NOTE: MockAutoScaler doesn't know the whole topo of tiflash_compute, it relies on endless case,
// so we only check if we can satisfy node_num, if not just return error.
func httpAssumeAndGetTopo(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	infoLogger.Printf("[http] got AssumeAndGetTopo request: %s", r.URL.String())

	nodeNumStr := r.FormValue("node_num")
	if nodeNumStr == "" {
		writeError(w, http.StatusBadRequest, "node_num is empty")
		return
	}
	nodeNum, err := strconv.Atoi(nodeNumStr)
	if err != nil {
		writeError(w, http.StatusBadRequest, fmt.Sprintf("node_num cannot convert to int. %s, %s", nodeNumStr, err.Error()))
		return
	}
	if nodeNum <= 0 {
		writeError(w, http.StatusBadRequest, "node_num cannot be zero")
		return
	}

	fullAddrs, errMsg := getEtcdTopo(etcdAddrsPath)
	if errMsg != "" {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	addrs := strings.Split(fullAddrs, topoSep)
	if len(addrs) < nodeNum {
		writeError(w, http.StatusInternalServerError, "node num too large, try to ScaleOut first")
		return
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fullAddrs))
	return
}

func httpReset(w http.ResponseWriter, r *http.Request) {
	mu.Lock()
	defer mu.Unlock()
	infoLogger.Printf("[http] got Reset http request: %s", r.URL.String())

	fullTopo, errMsg := getEtcdTopo(etcdAddrsPath)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}

	fullTenants, errMsg := getEtcdTopo(etcdTenantsPath)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}

	addrList := strings.Split(fullTopo, topoSep)
	tenantList := strings.Split(fullTenants, topoSep)

	if len(addrList) != len(tenantList) {
		errMsg := fmt.Sprintf("len(addrList)(%d) != len(tenantList)(%d)", len(addrList), len(tenantList))
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}

	for i := 0; i < len(addrList); i++ {
		tenantName := tenantList[i]
		infoLogger.Printf("reset %s, %s", addrList[i], tenantName)

		podIP, _, err := net.SplitHostPort(addrList[i])
		if err != nil {
			writeError(w, http.StatusInternalServerError, err.Error())
			return
		}
		grpcAddr := podIP + ":" + supervisorPort
		conn, err := grpc.Dial(grpcAddr, grpc.WithInsecure(), grpc.FailOnNonTempDialError(true), grpc.WithBlock())
		if err != nil {
			errMsg := fmt.Sprintf("dial grpc failed. addr: %s, err: %v", grpcAddr, err)
			writeError(w, http.StatusInternalServerError, errMsg)
			return
		}
		if conn != nil {
			defer conn.Close()
		}

		c := supervisor.NewAssignClient(conn)

		ctx, cancel := context.WithTimeout(context.Background(), 300*time.Second)
		defer cancel()

		grpcResp, err :=
			c.UnassignTenant(
				ctx,
				&supervisor.UnassignRequest{AssertTenantID: tenantName})
		if err != nil {
			errMsg := fmt.Sprintf("call UnassignTenant grpc failed. addr: %s, err: %v", grpcAddr, err)
			writeError(w, http.StatusInternalServerError, errMsg)
			return
		}
		if grpcResp.HasErr {
			errMsg := fmt.Sprintf("call UnassignTenant grpc return error. err: %s", grpcResp.ErrInfo)
			writeError(w, http.StatusInternalServerError, errMsg)
			return
		}
	}
}

func httpHomePage(w http.ResponseWriter, r *http.Request) {
	infoLogger.Printf("got homepage http request: %s", r.URL.String())
	w.WriteHeader(http.StatusNotFound)
	errMsg :=
		`Unexpected url path. You can try:
	1. ScaleOut tiflash_compute node: ip:port/scale_out_compute?pod_ip=x.x.x.x&tenant_name=x
	2. ScaleIn tiflash_compute node: ip:port/scale_in_compute?pod_ip=x.x.x.x&tenant_name=x
	3. GetTopo: ip:port/fetch_topo
`
	w.Write([]byte(errMsg))
	return
}

func startHttpServer(wg *sync.WaitGroup) *http.Server {
	wg.Add(1)
	srv := &http.Server{Addr:":"+mockAutoScalerPort}

	http.HandleFunc("/scale_out_compute", httpScaleOut)
	http.HandleFunc("/scale_in_compute", httpScaleIn)
	http.HandleFunc("/fetch_topo", httpFetchTopo)
	http.HandleFunc("/reset", httpReset)
	http.HandleFunc("/assume-and-get-topo", httpAssumeAndGetTopo)
	http.HandleFunc("/", httpHomePage)

	go func() {
		defer wg.Done()
		if err := srv.ListenAndServe(); err != http.ErrServerClosed {
			log.Fatalf("ListenAndServe(): %v", err)
		}
	}()
	return srv
}

func main() {
	// basePath := "/home/guojiangtao/tmp/tiflash-mock-autoscaler"
	basePath := "/tiflash-mock-autoscaler"
	initLogger(basePath)
	globalTopo = &computeTopo{
		addrs:   []string{},
		tenants: []string{},
	}

	// Etcd Server.
	etcdConfig := etcdembed.NewConfig()
	etcdConfig.Dir = path.Join(basePath, "etcd_data")
	etcdServer, err := etcdembed.StartEtcd(etcdConfig)
	if err != nil {
		log.Fatal(err)
	}
	defer etcdServer.Close()
	select {
	case <-etcdServer.Server.ReadyNotify():
		log.Printf("Server is ready!")
	case <-time.After(60 * time.Second):
		etcdServer.Server.Stop() // trigger a shutdown
		log.Printf("Server took too long to start!")
	}

	etcdCli, err = etcdcliv3.New(etcdcliv3.Config{
		Endpoints:   []string{"localhost:2379"},
		DialTimeout: 5 * time.Second,
	})
	if err != nil {
		log.Fatal(err)
	}

	sigs := make(chan os.Signal, 1)
	done := make(chan bool, 1)
	//registers the channel
	signal.Notify(sigs, syscall.SIGTERM)

	go func() {
		<-sigs
		log.Println("Caught SIGTERM, shutting down")
		done <- true
    }()

	// HTTP Server.
	httpSrvDone := &sync.WaitGroup{}
	httpSrv := startHttpServer(httpSrvDone)

	stopServers := func() {
		err := httpSrv.Shutdown(context.TODO())
		log.Println(err)
		httpSrvDone.Wait()
		etcdServer.Server.Stop()
	}

	select {
	case err = <-etcdServer.Err():
		log.Println(err)
		stopServers()
	case <-done:
		stopServers()
	}
}
