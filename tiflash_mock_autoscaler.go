package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"net/http"
	"os"
	"path"
	"strings"
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
	tiflashComputeTopoAddrsPath   = "/tiflash_compute_topo/addrs"
	tiflashComputeTopoTenantsPath = "/tiflash_compute_topo/tenants"
	supervisorPort                = "7000"
	defaultTiFlashPort            = "3930"
	topoSep                       = ";"
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
	if len(etcdGetResp.Kvs) != 1 {
		errMsg = fmt.Sprintf("unexpected etcd kv length, got: %d", len(etcdGetResp.Kvs))
		return
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
	infoLogger.Printf("put etcd succeed. resp: %v", etcdPutResp)
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
	fullTopo, errMsg := getEtcdTopo(tiflashComputeTopoAddrsPath)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	fullTenants, errMsg := getEtcdTopo(tiflashComputeTopoTenantsPath)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}

	addedTiFlashAddr := podIP + ":" + defaultTiFlashPort
	if len(fullTopo) == 0 {
		fullTopo = addedTiFlashAddr
		fullTenants = tenantName
		infoLogger.Printf("etcd is empty, will add %s", addedTiFlashAddr)
	} else {
		infoLogger.Printf("etcd is not empty, ori is: %s, will add %s", fullTopo, addedTiFlashAddr)
		fullTopo += topoSep + addedTiFlashAddr
		fullTenants += topoSep + tenantName
	}

	putEtcdTopo(tiflashComputeTopoAddrsPath, fullTopo)
	putEtcdTopo(tiflashComputeTopoTenantsPath, fullTenants)

	w.WriteHeader(http.StatusOK)
	return
}

func httpScaleIn(w http.ResponseWriter, r *http.Request) {
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

	fullTopo, errMsg := getEtcdTopo(tiflashComputeTopoAddrsPath)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	fullTenants, errMsg := getEtcdTopo(tiflashComputeTopoTenantsPath)
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

	putEtcdTopo(tiflashComputeTopoAddrsPath, newFullTopo)
	putEtcdTopo(tiflashComputeTopoTenantsPath, newFullTenants)

	w.WriteHeader(http.StatusOK)
	return
}

func httpGetTopo(w http.ResponseWriter, r *http.Request) {
	infoLogger.Printf("[http] got GetTopo http request: %s", r.URL.String())

	var fullTopo string
	etcdGetResp, err := etcdcliv3.NewKV(etcdCli).Get(context.TODO(), tiflashComputeTopoAddrsPath)
	if err != nil {
		errMsg := fmt.Sprintf("get etcd failed. err: %s", err.Error())
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}
	if len(etcdGetResp.Kvs) > 1 {
		errMsg := fmt.Sprintf("unexpected etcd kv length, got: %d", len(etcdGetResp.Kvs))
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	} else if len(etcdGetResp.Kvs) == 0 {
		infoLogger.Printf("etcd is empty")
	} else {
		fullTopo = string(etcdGetResp.Kvs[0].Value)
		infoLogger.Printf("etcd is not empty, fullTopo is: %s", fullTopo)
	}
	w.WriteHeader(http.StatusOK)
	w.Write([]byte(fullTopo))
	return
}

func httpReset(w http.ResponseWriter, r *http.Request) {
	infoLogger.Printf("[http] got Reset http request: %s", r.URL.String())

	fullTopo, errMsg := getEtcdTopo(tiflashComputeTopoAddrsPath)
	if len(errMsg) != 0 {
		writeError(w, http.StatusInternalServerError, errMsg)
		return
	}

	fullTenants, errMsg := getEtcdTopo(tiflashComputeTopoTenantsPath)
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
	3. GetTopo: ip:port/get_topo
`
	w.Write([]byte(errMsg))
	return
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

	// HTTP Server.
	go func() {
		// ip:port/scale_out_compute?pod_ip=x.x.x.x&tenant_name=x
		http.HandleFunc("/scale_out_compute", httpScaleOut)
		http.HandleFunc("/scale_in_compute", httpScaleIn)
		http.HandleFunc("/get_topo", httpGetTopo)
		http.HandleFunc("/reset", httpReset)
		http.HandleFunc("/", httpHomePage)
		// TODO: gracefule shutdown.
		http.ListenAndServe(":"+mockAutoScalerPort, nil)
	}()

	select {
	case err = <-etcdServer.Err():
		log.Fatal(err)
	}
}
