package main

// standard imports
import (
	"bytes"
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/gorilla/mux"
)

// message struct is used to unpack request vals into a struct that can handle null causal metadata
type message struct {
	Value          interface{}  `json:"value"`
	CausalMetadata *ReqMetaData `json:"causal-metadata"`
}

// reqMetaData is used to unpack request vals when they actually exist and are not null so they can be easily assigned a type
type ReqMetaData struct {
	ReqVector       []int `json:"ReqVector, omitempty"`
	ReqIpIndex      int   `json:"ReqIpIndex, omitempty"`
	IsReqFromClient bool  `json:"IsReqFromClient, omitempty"`
}

// declaring our Vector Clock, which we'll use for causal consistency
type VectorClock struct {
	VC []int `json:"VC, omitempty"`
}

var replicaArray []string // holds IP's of all replicas
var replicaCount = 0      // local Counter for number of replicas online
var sAddress string       // socket address
var viewArray []string    // array of IP's currently in view i.e. online
var vectorIndex = -1      // represents which index in replicaArray this current thread is
var shardCount = -1       // represents # of shards we are given at start of program

var shardSplit = make(map[string][]string)
var ipToShardMap = make(map[string]string)
var hashIndexArr []string
var currentShard string

// first 3 integers represent the vector clock of the local replica
// 4th vector is the index of the Ip that all replicas have access to
// 5th vector is the binary toggle (1 or 0) that determines if the data was from a client (1) or a replica (0)
var localVector []int

// Used in mapping replica IP to index of the replicaArray
var ipToIndex = make(map[string]int)

// our local KVS store
var store = make(map[string]interface{})

func main() {
	r := mux.NewRouter()

	//testing purposes
	//os.Setenv("SOCKET_ADDRESS", "10.10.0.2:8090")
	//os.Setenv("VIEW", "10.10.0.2:8090,10.10.0.3:8090,10.10.0.4:8090")

	//pulls unique replica address from env variable
	sAddress = os.Getenv("SOCKET_ADDRESS")

	// grabbing env variables that are passed in
	vAddresses := os.Getenv("VIEW")
	shardCountString := os.Getenv("SHARD_COUNT")

	shardCountOutput, err := strconv.Atoi(shardCountString)
	if err != nil {
		fmt.Println("error converting string to int = ", err)
	}
	shardCount = shardCountOutput
	replicaArray = strings.Split(vAddresses, ",")
	viewArray = strings.Split(vAddresses, ",")

	for index, ip := range viewArray {
		ipToIndex[ip] = index
		localVector = append(localVector, 0)
		if ip == sAddress {
			vectorIndex = index
		}
	}

	fmt.Println("ip to index == ", ipToIndex)
	fmt.Println("vector index ===", vectorIndex)
	fmt.Println("localVector === ", localVector)

	// Handlers for each scenario of input for URL
	r.HandleFunc("/view", handleView)
	r.HandleFunc("/kvs/{key}", handleKey)
	r.HandleFunc("/getVC", handleGetVC)
	r.HandleFunc("/getKVS", handleGetKVS)

	// Handlers for sharding requests
	r.HandleFunc("/shard/ids", handleShardAllId)
	r.HandleFunc("/shard/node-shard-id", handleShardOneId)
	r.HandleFunc("/shard/members/{id}", handleShardMembers)
	r.HandleFunc("/shard/key-count/{id}", handleShardKeyCount)
	r.HandleFunc("/shard/add-member/{id}", handleShardAddMember)
	r.HandleFunc("/shard/reshard", handleReshard)
	r.HandleFunc("/shard/broadcast/add/{id}", handleBroadcastedAdd)

	// function that checks if this replica has just died
	//go didIDie()

	splitNodes(shardCount)
	currentShard = ipToShardMap[sAddress]
	fmt.Println("current shard = ", currentShard)

	// Service listens on port 8090
	log.Fatal(http.ListenAndServe(":8090", r))
}

func forwardReq(rMethod string, rIP string, rPath string, rBody []byte) ([]byte, int) {
	client := &http.Client{}
	// Creating new request
	req, err := http.NewRequest(rMethod, fmt.Sprintf("http://%s%s", rIP, rPath), bytes.NewBuffer(rBody))
	if err != nil {
		log.Fatalf("problem creating new http request: %s", err)
	}
	// Forwarding the new request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Printf("%s is down due to: %s", rIP, err)
	}
	// Closing body of resp, typical after using Client.do()
	//defer resp.Body.Close()
	if resp != nil {
		returnBytes, err := io.ReadAll(resp.Body)
		if err != nil {
			log.Fatal(err)
		}
		return returnBytes, resp.StatusCode
	}
	return make([]byte, 0), http.StatusBadRequest
}

func compareSlices(s1 []int, s2 []int) bool {
	for i, v := range s2 {
		if v != s1[i] {
			return false
		}
	}
	return true
}

func hash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32()) % shardCount
}

func getShardKeyCount(ipInShard string, shardId string) []byte {
	res, err := http.Get(fmt.Sprintf("http://%s/shard/key-count/%s", ipInShard, shardId))
	if err != nil {
		fmt.Println("problem creating new http request")
	}
	fmt.Println(shardId, "outside", currentShard, "scope. grabbing key count from", ipInShard)
	// decoding the response of new request
	returnBytes, err := io.ReadAll(res.Body)
	if err != nil {
		log.Fatal(err)
	}
	return returnBytes
}

func findKey(m map[string][]string, value string) (key string, ok bool) {
	for k, v := range m {
		if containsVal(value, v) >= 0 {
			key = k
			ok = true
			return
		}
	}
	return
}

func splitNodes(shardAmount int) {
	shardSplitArray := make([][]string, 0)
	shardCount = shardAmount
	hashIndexArr = nil

	for i := 0; i < shardAmount; i++ {
		shardSplitArray = append(shardSplitArray, make([]string, 0))
	}

	for i := 0; i < len(viewArray); i++ {
		x := i % shardAmount
		shardSplitArray[x] = append(shardSplitArray[x], viewArray[i])
		ipToShardMap[viewArray[i]] = "s" + strconv.Itoa(x)
	}

	for i := 0; i < shardCount; i++ {
		shardName := "s" + strconv.Itoa(i)
		shardSplit[shardName] = shardSplitArray[i]
		hashIndexArr = append(hashIndexArr, shardName)
	}
	fmt.Println("shardSplitArray ===", shardSplitArray)
	fmt.Println("shardSplit ===", shardSplit)
	fmt.Println("hashIndex arr", hashIndexArr)
	fmt.Println("\n\n ip to shard mapping === \n", ipToShardMap)

}

// Used to check if current replica has just died
func didIDie() {
	// sleep for a second, so that we can confirm other replicas have time to start up
	time.Sleep(time.Second * 1)
	// checking all elements of current view
	for _, replicaIP := range viewArray {
		// if any in the view is not our address
		if replicaIP != sAddress {
			// gets vector clock of that other replica
			var repVC = getReplicaVectorClock(replicaIP)
			fmt.Println("repVC === ", repVC)

			// if other replica's VC is not equal to our own
			if !compareSlices(repVC, localVector) {
				//set local VC to grabbed VC
				localVector = repVC
				//we know we died and need to grab the new KVS
				store = getReplicaKVS(replicaIP)
				//and push our Ip to the replica Array
				pushIpToReplicas(sAddress)
			}
		}
	}
}

// Function used to send our IP to a replica's view array
func pushIpToReplicas(replicaIP string) {
	// making a response, and map our socket address
	response := make(map[string]string)
	response["socket-address"] = sAddress

	// standard json marshal of repsonse
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}

	// checking each replica IP of all replicas
	for _, replicaIP := range replicaArray {
		// if the replica IP is not our own
		if replicaIP != sAddress {
			client := &http.Client{}
			// Creating new request to PUT our IP in the replica's view array
			req, err := http.NewRequest("PUT", fmt.Sprintf("http://%s/view", replicaIP), bytes.NewBuffer(jsonResponse))
			if err != nil {
				fmt.Println("problem creating new http request")
			}

			// Forwarding the new request
			resp, err := client.Do(req)
			if err != nil {
				fmt.Println(replicaIP, " is down due to: ", err)
				return
			}
			defer resp.Body.Close()
		}
	}

}

// Function used to get the kvs store of another replica
func getReplicaKVS(replicaIP string) map[string]interface{} {
	var response map[string]interface{}

	// Creating new request
	res, err := http.Get(fmt.Sprintf("http://%s/getKVS", replicaIP))
	if err != nil {
		fmt.Println("problem creating new http request")
	}

	// decoding the response of new request
	decodeError := json.NewDecoder(res.Body).Decode(&response)
	if decodeError != nil {
		log.Fatalf("Error: %s", err)
	}

	// returning the KVS
	return response["KVS"].(map[string]interface{})
}

// Function used  to get the vector clock of another replica
func getReplicaVectorClock(replicaIP string) []int {
	var response VectorClock

	// Creating new request
	fmt.Println("replicaIP ==== ", replicaIP)
	res, err := http.Get(fmt.Sprintf("http://%s/getVC", replicaIP))
	if err != nil {
		fmt.Println("problem creating new http request here")
		log.Fatalf("Error: %s", err)
	}

	// decoding the response of new request
	decodeError := json.NewDecoder(res.Body).Decode(&response)
	if decodeError != nil {
		log.Fatalf("Error: %s", err)
	}

	// returning the VC from other replica
	return response.VC
}

// Helper function used to check if the database has been changed
func isDatabaseChanged(response map[string]interface{}) bool {

	//check if a value actually got added to db, and if so we need to alert the other replicas
	if _, ok := response["result"]; ok {
		val := response["result"]
		if val == "created" || val == "updated" || val == "deleted" {
			return true
		}
	}
	return false
}

// Helper function used to broadcast a message to a replica
func broadcastMessage(replicaIP string, req *http.Request, updatedBody []byte) {

	client := &http.Client{}
	fmt.Println("req method: ", req.Method)
	fmt.Println("req URL: ", fmt.Sprintf("http://%s%s", replicaIP, req.URL.Path))

	// Creating new request
	req, err := http.NewRequest(req.Method, fmt.Sprintf("http://%s%s", replicaIP, req.URL.Path), bytes.NewBuffer(updatedBody))
	if err != nil {
		fmt.Println("problem creating new http request")
		return
	}

	// Checking to see if we can even reach that IP
	reachedURL, err := net.DialTimeout("tcp", replicaIP, (1 * time.Second))
	// If we can't reach that Replica, we know that it is down
	// Therefore, we must then broadcast and update views of all replicas
	if err != nil {
		fmt.Println(replicaIP, " is down! didnt reply within 2 seconds")

		// finding the index of the replica in array of online replicas
		i := containsVal(replicaIP, replicaArray)
		if i >= 0 {
			// removing that index from the array of online replicas
			replicaArray = removeVal(i, replicaArray)
		}
		fmt.Println("view is now ===", replicaArray)

		// Looping thru all replica IPs in replicaArray
		for _, repIP := range replicaArray {
			// if that replica IP is not the one we are broadcasting to, and is not our current replica
			if repIP != replicaIP && repIP != sAddress {
				fmt.Println("tell ", repIP, "that ", replicaIP, " is down")

				// making a variable to hold replicaIP
				viewBody := map[string]string{
					"socket-address": replicaIP,
				}

				// standard json marshalling
				viewBodyJson, err := json.Marshal(viewBody)
				if err != nil {
					log.Fatalf("Error: %s", err)
				}

				// checking if repIP is reachable
				reachedURL, err := net.DialTimeout("tcp", repIP, (2 * time.Second))
				if err != nil {
					fmt.Println("couldnt reach for DELETE", repIP, "err ===", err)
					return
				}

				// Creating new delete request to deletethe down replica IP from a replica's view
				reachedURL.Close()
				viewReq, err := http.NewRequest("DELETE", fmt.Sprintf("http://%s/view", repIP), bytes.NewBuffer(viewBodyJson))
				if err != nil {
					fmt.Println("Error broadcasting view: delete to replicas")
					return
				}

				// Sending delete request to the replica
				res, err := client.Do(viewReq)
				if err != nil {
					fmt.Println("problem creating new http request for view delete broadcast")
					return
				}
				defer res.Body.Close()
				//fmt.Println("delete body == ", res.Body)
			}
		}
		return
	}
	reachedURL.Close()

	// Forwarding the new request
	resp, err := client.Do(req)
	if err != nil {
		fmt.Println(replicaIP, " is down due to: ", err)
		return
	}
	// Closing body of resp, typical after using Client.do()
	defer resp.Body.Close()
	return
}

// Helper function to check if an array contains a certain value, and at what index
func containsVal(val string, repArray []string) int {
	for index, a := range repArray {
		if a == val {
			return index
		}
	}
	return -1
}

// Helper function to remove a Value from a certain string array
func removeVal(index int, repArray []string) []string {
	repArray[index] = repArray[len(repArray)-1]
	return repArray[:len(repArray)-1]
}

func handleBroadcastedAdd(w http.ResponseWriter, req *http.Request) {
	fmt.Println("broadcast recieved")
	param := mux.Vars(req)
	id := param["id"]
	var reqVals map[string]string
	err := json.NewDecoder(req.Body).Decode(&reqVals)
	if err != nil {
		log.Fatalf("Error couldnt decode: %s", err)
		return
	}
	shardSplit[id] = append(shardSplit[id], reqVals["add-ip"])
	fmt.Println("shard split updated to: ", shardSplit)
	w.WriteHeader(http.StatusOK)
}

// Handler Function that handles when we are given a request to return
// our local VC, which we send out as a JSON object
func handleGetVC(w http.ResponseWriter, req *http.Request) {
	response := make(map[string]interface{})

	if req.Method == "GET" {
		response["VC"] = localVector
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	w.Write(jsonResponse)
}

// Handler functuon that handles when we are guven a request to return
// our local KVS, which we send out as json object
func handleGetKVS(w http.ResponseWriter, req *http.Request) {
	response := make(map[string]interface{})

	if req.Method == "GET" {
		response["KVS"] = store
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error: %s", err)
	}
	w.Write(jsonResponse)
}

// Handler function that handles all operations wheb we are  given
// requests regarding our KVS
func handleKey(w http.ResponseWriter, req *http.Request) {

	// grabbing params to be used
	param := mux.Vars(req)
	key := param["key"]

	// initilizations of necessary variables
	response := make(map[string]interface{})
	broadcastResponse := make(map[string]interface{})
	var responseMetadata ReqMetaData

	// create dict variable to hold inputted value
	var reqVals message

	fmt.Println("req.Body === \n", req.Body)

	// handles pulling out and storing value into newVal
	err := json.NewDecoder(req.Body).Decode(&reqVals)
	if err != nil {
		log.Fatalf("Error couldnt decode: %s", err)
		return
	}

	//check which shard it belongs to
	hashedKeyIndex := hash(key)
	//if true key belongs to shard we are in, so handle accordingly
	//if false, key belongs to another shard, so forawr request to first ip in correct shard
	if hashIndexArr[hashedKeyIndex] == ipToShardMap[sAddress] {
		fmt.Println("hashing at: ", sAddress)

		// assigning metadata from our request
		metadata := reqVals.CausalMetadata

		fmt.Println("localvector on recieve === ", localVector)

		// If metadata is not empty, we  know that this is not first interaction with client
		if metadata != nil {
			reqVector := metadata.ReqVector
			responseMetadata.IsReqFromClient = metadata.IsReqFromClient
			fmt.Println("vector clock from request === ", reqVector)
			//fmt.Println("IP index === ", metadata.ReqIpIndex)
			//fmt.Println("req from client? === ", metadata.IsReqFromClient)

			//check for consistency violations
			for i := 0; i < len(reqVector); i++ {
				if metadata.IsReqFromClient {
					if reqVector[i] > localVector[i] {
						//consistency violation
						//fmt.Println("bigger in the ", i, " position")
						w.WriteHeader(http.StatusServiceUnavailable)
						response["error"] = "Causal dependencies not satisfied; try again later"
					}
				} else {
					if i == metadata.ReqIpIndex {
						if reqVector[i] != localVector[i]+1 {
							//consistency violation
							//fmt.Println("bigger in the metadata.repipindex position: ", reqVector[i], " != ", localVector[i]+1, " when i = ", i)
							w.WriteHeader(http.StatusServiceUnavailable)
							response["error"] = "Causal dependencies not satisfied; try again later"
						}
					} else if reqVector[i] > localVector[i] {
						//consistency violation
						//fmt.Println("bigger in the ", i, " position")
						w.WriteHeader(http.StatusServiceUnavailable)
						response["error"] = "Causal dependencies not satisfied; try again later"
					}
				}
			}

			// if no causal dependency is detected, set the local clock to the max of the local clock and request clock
			if _, violation := response["error"]; !violation {
				if req.Method != "GET" {
					for i := 0; i < len(reqVector); i++ {
						if reqVector[i] > localVector[i] {
							localVector[i] = reqVector[i]
						}
					}
				}
			}

		} else {
			//handling inital nil case all future client requests will increment local clock when getting the max
			responseMetadata.IsReqFromClient = true
		}

		if _, violation := response["error"]; !violation {

			// PUT case
			if req.Method == "PUT" {

				val := reqVals.Value
				// handling cases where user input is:
				// 1. invalid (key too long)
				// 2. invalid (no value specified)
				// 3. being replaced (key already exists)
				// 4. being created (key does not exist)
				if len(key) > 50 {
					w.WriteHeader(http.StatusBadRequest)
					response["error"] = "Key is too long"
				} else if val == nil {
					w.WriteHeader(http.StatusBadRequest)
					response["error"] = "PUT request does not specify a value"
				} else if _, ok := store[key]; ok {
					w.WriteHeader(http.StatusOK)
					response["result"] = "updated"
					store[key] = val
				} else {
					w.WriteHeader(http.StatusCreated)
					response["result"] = "created"
					store[key] = val
				}

				// GET case
			} else if req.Method == "GET" {

				// handling cases where user input is:
				// 1. valid (key exists)
				// 2. invalid (key does not exist)
				if _, ok := store[key]; ok {
					w.WriteHeader(http.StatusOK)
					response["result"] = "found"
					response["value"] = store[key]
				} else {
					w.WriteHeader(http.StatusNotFound)
					response["error"] = "Key does not exist"
				}

				// DELETE case
			} else if req.Method == "DELETE" {

				// handling cases where user input is;
				// 1. valid (key exists)
				// 2. invalid (key does not exist)
				if _, ok := store[key]; ok {
					w.WriteHeader(http.StatusOK)
					response["result"] = "deleted"
					delete(store, key)
				} else {
					w.WriteHeader(http.StatusNotFound)
					response["error"] = "Key does not exist"
				}
			}

			// reassigning necessary values in our response metadata
			responseMetadata.ReqVector = localVector
			responseMetadata.ReqIpIndex = ipToIndex[sAddress]

			// checking if we changed our database, and if so, to increment VC
			if isDatabaseChanged(response) {
				// Incrementing if the request is from the client
				if responseMetadata.IsReqFromClient {
					localVector[vectorIndex]++
				}

				//update response to updated clock index
				responseMetadata.ReqVector = localVector

				//if from client we need to broadcast req to other replicas
				//if from replica, we dont do anything here
				if responseMetadata.IsReqFromClient {
					var broadcastMetadata ReqMetaData
					broadcastMetadata.ReqVector = localVector
					broadcastMetadata.ReqIpIndex = ipToIndex[sAddress]
					broadcastMetadata.IsReqFromClient = false
					broadcastResponse["value"] = reqVals.Value
					broadcastResponse["causal-metadata"] = broadcastMetadata
					updatedBody, err := json.Marshal(broadcastResponse)
					if err != nil {
						log.Fatalf("response not marshalled: %s", err)
						return
					}

					fmt.Println("updated body ===", string(updatedBody))

					//broadcast to other replicas
					for _, replicaIP := range shardSplit[currentShard] {
						if replicaIP != sAddress {
							//need to update so that we only broadcast to replicas inside our own shard
							broadcastMessage(replicaIP, req, updatedBody)
						}
					}
				}
			}

			//set responses metadata to updated metadata
			response["causal-metadata"] = responseMetadata
			response["shard-id"] = ipToShardMap[sAddress]
		}
		fmt.Println("localvector after request is processed === ", localVector)
		fmt.Println("view after kvs update === ", replicaArray)
		// sending correct response / status code back to client
		fmt.Println("response === ", response)
		jsonResponse, err := json.Marshal(response)
		if err != nil {
			log.Fatalf("Error: %s", err)
		}
		w.Write(jsonResponse)
	} else {
		forwardRequest := make(map[string]interface{})
		forwardRequest["value"] = reqVals.Value
		forwardRequest["causal-metadata"] = reqVals.CausalMetadata
		marshalledRequest, err := json.Marshal(forwardRequest)
		if err != nil {
			log.Fatalf("couldnt marshall: %s", err)
		}
		shardToForwardRequest := hashIndexArr[hashedKeyIndex]
		replicaIP := shardSplit[shardToForwardRequest][0]

		fmt.Println("key hashed to ", shardToForwardRequest, " sending to ", replicaIP)
		jsonResponse, statusCode := forwardReq(req.Method, replicaIP, req.URL.Path, marshalledRequest)
		w.WriteHeader(statusCode)
		w.Write(jsonResponse)
	}
}

// Handler function that handles all program behavior regarding view operations
func handleView(w http.ResponseWriter, req *http.Request) {

	response := make(map[string]interface{})

	// create dict variable to hold inputted value
	var newVal map[string]string

	if req.Method == "PUT" {

		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&newVal)
		if err != nil {
			log.Fatalf("Error: %s", err)
			return
		}
		val := newVal["socket-address"]

		// auto adding if this is first replica
		if replicaCount == 0 {
			replicaArray = append(replicaArray, val)
			w.WriteHeader(http.StatusCreated)
			response["result"] = "added"
			replicaCount++
		} else {
			// checking to make sure entry is already  present
			if containsVal(val, replicaArray) >= 0 {
				w.WriteHeader(http.StatusOK)
				response["result"] = "already present"
			} else {
				replicaArray = append(replicaArray, val)
				w.WriteHeader(http.StatusCreated)
				response["result"] = "added"
				replicaCount++
			}
		}
	} else if req.Method == "GET" {
		// simply returning replica array for view
		w.WriteHeader(http.StatusOK)
		response["view"] = replicaArray

	} else if req.Method == "DELETE" {
		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&newVal)
		if err != nil {
			log.Fatalf("Error: %s", err)
			return
		}
		val := newVal["socket-address"]

		// finding index of the value in replica array
		index := containsVal(val, replicaArray)
		// if it is found
		if index >= 0 {
			// delete the replica from view
			replicaArray = removeVal(index, replicaArray)
			w.WriteHeader(http.StatusCreated)
			response["result"] = "deleted"
			replicaCount--
		} else {
			// returning when replica is not found
			w.WriteHeader(http.StatusNotFound)
			response["error"] = "View has no such replica"
		}
	}

	fmt.Println("view after view update === ", replicaArray)
	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}
	w.Write(jsonResponse)

}

func handleShardAllId(w http.ResponseWriter, req *http.Request) {
	allId := make([]string, 0)
	response := make(map[string]interface{})

	if req.Method == "GET" {
		for key := range shardSplit {
			allId = append(allId, key)
		}
		w.WriteHeader(http.StatusOK)
		response["shard-ids"] = allId
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}
	w.Write(jsonResponse)

}

func handleShardOneId(w http.ResponseWriter, req *http.Request) {

	response := make(map[string]interface{})

	if req.Method == "GET" {
		w.WriteHeader(http.StatusOK)
		response["node-shard-id"] = currentShard
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}
	w.Write(jsonResponse)

}

func handleShardMembers(w http.ResponseWriter, req *http.Request) {
	param := mux.Vars(req)
	id := param["id"]

	response := make(map[string]interface{})

	if req.Method == "GET" {
		if shardArray, ok := shardSplit[id]; ok {
			w.WriteHeader(http.StatusOK)
			response["shardMembers"] = shardArray
		} else {
			w.WriteHeader(http.StatusNotFound)
		}
	}

	jsonResponse, err := json.Marshal(response)
	if err != nil {
		log.Fatalf("Error here: %s", err)
	}
	w.Write(jsonResponse)

}

func handleShardKeyCount(w http.ResponseWriter, req *http.Request) {
	param := mux.Vars(req)
	id := param["id"]

	response := make(map[string]interface{})

	if req.Method == "GET" {
		if currentShard == id {
			w.WriteHeader(http.StatusOK)
			response["shard-key-count"] = len(store)

			jsonResponse, err := json.Marshal(response)
			if err != nil {
				log.Fatalf("Error here: %s", err)
			}
			w.Write(jsonResponse)
		} else {
			//check if id is even a shard
			if _, ok := shardSplit[id]; !ok {
				w.WriteHeader(http.StatusNotFound)
			} else {
				//key isnt equal to id, meaning this replica is not a member of the shard we want the key count of

				//grab first ip thats a member of desired shard
				ipInShard := shardSplit[id][0]
				forwardResponse := getShardKeyCount(ipInShard, id)
				w.Write(forwardResponse)
			}
		}

	}

}

func handleShardAddMember(w http.ResponseWriter, req *http.Request) {
	param := mux.Vars(req)
	reqId := param["id"]

	// create dict variable to hold inputted value
	var reqVals map[string]string
	response := make(map[string]string)

	if req.Method == "PUT" {
		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&reqVals)
		if err != nil {
			log.Fatalf("Error couldnt decode: %s", err)
			return
		}
		reqIp := reqVals["socket-address"]

		if _, ok := shardSplit[reqId]; !ok || containsVal(reqIp, replicaArray) < 0 {
			w.WriteHeader(http.StatusNotFound)
		} else {
			//add node to shard and update all other replicas in view
			w.WriteHeader(http.StatusOK)
			shardSplit[reqId] = append(shardSplit[reqId], reqIp)
			broadcastBody := make(map[string]string)
			broadcastBody["add-ip"] = reqIp
			bBody, err := json.Marshal(broadcastBody)
			if err != nil {
				log.Fatalf("Error here: %s", err)
			}
			for _, replicaIp := range replicaArray {
				if replicaIp != sAddress {
					fmt.Println("forwarding to ", replicaIp)
					forwardReq("PUT", replicaIp, fmt.Sprintf("/shard/broadcast/add/%s", reqId), bBody)
				}
			}
			response["result"] = "node added to shard"
			jsonResponse, err := json.Marshal(response)
			if err != nil {
				log.Fatalf("Error here: %s", err)
			}
			w.Write(jsonResponse)
		}
	}
	
}

func handleReshard(w http.ResponseWriter, req *http.Request) {
	// create dict variable to hold inputted value
	var reqVals map[string]string
	response := make(map[string]string)

	if req.Method == "PUT" {
		// handles pulling out and storing value into newVal
		err := json.NewDecoder(req.Body).Decode(&reqVals)
		if err != nil {
			log.Fatalf("Error couldnt decode: %s", err)
			return
		}
		
		// grabbing shardcount from request json body
		newShardCount, err := strconv.Atoi(reqVals["shard-count"])
		if err != nil {
			fmt.Println("error converting string to int = ", err)
		}

		// first checking if fault tolerance invariant is violated
		// i.e. too many shards and too few replicas
		if float32(replicaCount) / float32(newShardCount) < 2.0 {
			response["error"] = "Not enough nodes to provide fault tolerance with requested shard count"
			w.WriteHeader(http.StatusBadRequest)
		} else{
			// creating new store variable to hold the result of combining all the stores of all shards
			entireStore := make(map[string]interface{})

			// combining our current replica's shard's store into the new entireStore variable
			for k, v := range store {
				entireStore[k] = v
			}

			// loop thru all shards, and their respective replica IP arrays
			for key, element := range shardSplit {
				// finding a shard that is not the one we are on
				// in this case, we must send a request to one of the replicas in that shard to give us their kvs
        		if key != currentShard {
					// getting the kvs of the first replicaIP in the shard's replica IP array
					tempStore := getReplicaKVS(element[0])

					// combining the entireStore with the store we got from the replica
					for k, v := range tempStore {
						entireStore[k] = v
					}
				}
    		}

		}

		// writing json response
		jsonResponse, err := json.Marshal(response)
		if err != nil {
			log.Fatalf("Error here: %s", err)
		}
		w.Write(jsonResponse)
	}

}
