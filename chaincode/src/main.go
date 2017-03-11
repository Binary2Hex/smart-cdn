package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/google/uuid"
	"github.com/hyperledger/fabric/core/chaincode/shim"
)

// CDNManager example simple Chaincode implementation
type CDNManager struct {
}

var TASK_IDS = "TaskIDs"

var NODE_PREFIX = "node:"

type CDNNode struct {
	Name  string   `json:"name"`
	Score int      `json:"score"`
	IP    string   `json:"ip"`
	Tasks []string `json:"tasks"`
}

var TASK_PREFIX = "task:"

// This should be named as resouce
type Task struct {
	ID       string   `json:"id"`
	Customer string   `json:"customer"`
	Nodes    []string `json:"nodes"`
	Size     string   `json:"size"`
	Type     string   `json:"type"`
	URL      string   `json:"url"`
	Time     int64    `json:"time"`
}

var VISITED_PREFIX = "visited:"

type ResouceVisitRecord struct {
	Time        int64  `json:"time"`
	TaskID      string `json:"taskId"`
	CDNNodeName string `json:"cdnNodeName"`
	EndpointIP  string `json:"endpointIP"`
	Size        int    `json:"size"`
	Ack         int    `json:"ack"`
}

func main() {
	err := shim.Start(new(CDNManager))
	if err != nil {
		fmt.Printf("Error starting Simple chaincode:", err)
	}
}

var NODE1 = []byte("cdn-node-cn.mybluemix.net")
var NODE2 = []byte("cdn-node-uk.mybluemix.net")

// Init resets all the things
func (t *CDNManager) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}

	// Initialize the collection of task IDs
	fmt.Println("Initialize task IDs collection")
	var blank []string
	blankBytes, err := json.Marshal(&blank)
	if err != nil {
		fmt.Println("Error marshalling ids")
		return nil, err
	}

	err = stub.PutState(TASK_IDS, blankBytes)
	if err != nil {
		fmt.Println("Failed to initialize task IDs collection")
	}

	// err = t.initSamples(stub)

	fmt.Println("Initialization complete")
	return nil, nil
}

func (t *CDNManager) initSamples(stub shim.ChaincodeStubInterface) error {
	// Initialize few tasks
	task1 := Task{ID: "001", Customer: "IBM", URL: "https://www.ibm.com/us-en/images/homepage/featured/02032017_f_arrowhead_15894_600x260.jpg"}
	task2 := Task{ID: "002", Customer: "Baidu", URL: "https://ss0.bdstatic.com/5aV1bjqh_Q23odCf/static/superman/img/logo/bd_logo1_31bdc765.png"}
	task3 := Task{ID: "003", Customer: "Tudo", URL: "http://www.tudou.com/favicon.ico"}
	task4 := Task{ID: "004", Customer: "Youtube", URL: "http://www.youtube.com/favicon.ico"}
	task1Bytes, err1 := json.Marshal(task1)
	task2Bytes, err2 := json.Marshal(task2)
	task3Bytes, err3 := json.Marshal(task3)
	task4Bytes, err4 := json.Marshal(task4)
	if err1 != nil {
		fmt.Println("Error Marshal task1")
		return err1
	}
	if err2 != nil {
		fmt.Println("Error Marshal task2")
		return err2
	}
	if err3 != nil {
		fmt.Println("Error Marshal task3")
		return err3
	}
	if err4 != nil {
		fmt.Println("Error Marshal task4")
		return err4
	}
	err1 = stub.PutState(TASK_PREFIX+task1.ID, task1Bytes)
	t.updateTaskIDList(stub, task1.ID)
	err2 = stub.PutState(TASK_PREFIX+task2.ID, task2Bytes)
	t.updateTaskIDList(stub, task2.ID)
	err3 = stub.PutState(TASK_PREFIX+task3.ID, task3Bytes)
	t.updateTaskIDList(stub, task3.ID)
	err4 = stub.PutState(TASK_PREFIX+task4.ID, task4Bytes)
	t.updateTaskIDList(stub, task4.ID)

	return err1
}

// Invoke isur entry point to invoke a chaincode function
func (t *CDNManager) Invoke(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	fmt.Println("invoke is running " + function)

	// Handle different functions
	if function == "init" {
		return t.Init(stub, "init", args)
	} else if function == "submitTask" {
		return t.submitTask(stub, args)
	} else if function == "registerCDNNode" {
		return t.registerCDNNode(stub, args)
	} else if function == "claimTask" {
		return t.claimTask(stub, args)
	} else if function == "recordVisit" {
		return nil, t.recordVisit(stub, args)
	} else if function == "confirmRecordVisit" {
		return nil, t.confirmRecordVisit(stub, args)
	}
	fmt.Println("invoke did not find func: " + function)

	return nil, errors.New("Received unknown function invocation: " + function)
}

// Query is our entry point for queries
func (t *CDNManager) Query(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	fmt.Println("query is running " + function)

	// Handle different functions
	if function == "getTaskList" {
		fmt.Println("Getting Task List")
		taskList, err := t.getTaskList(stub)
		if err != nil {
			fmt.Println("Error from getTaskList")
			return nil, err
		}
		taskListBytes, err1 := json.Marshal(&taskList)
		if err1 != nil {
			fmt.Println("Error marshalling taskList")
			return nil, err1
		}
		fmt.Println("All success, returning taskList")
		return taskListBytes, nil
	} else if function == "locateCDN" {
		fmt.Println("Getting node by task ID")
		nodeIP, err := t.locateCDN(stub, args)
		return nodeIP, err
	} else if function == "getNodeList" {
		fmt.Println("Getting node list")
		nodeList, err := t.getNodeList(stub)
		if err != nil {
			fmt.Println("Error getting node list", err)
			return nil, err
		}
		nodeListBytes, err1 := json.Marshal(&nodeList)
		if err1 != nil {
			fmt.Println("Error marshalling node list", err1)
			return nil, err1
		}
		return nodeListBytes, err
	} else if function == "getReport" {
		fmt.Println("Getting report")
		return t.getReport(stub, args)
	}
	fmt.Println("query did not find func: " + function)

	return nil, errors.New("Received unknown function query: " + function)
}

func (t *CDNManager) registerCDNNode(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	var node CDNNode
	err := json.Unmarshal([]byte(args[0]), &node)
	if err != nil {
		fmt.Println("Error unmarshal cdn node", err)
		return nil, err
	}
	return nil, t.saveCDNNode(stub, &node)
}

func (t *CDNManager) submitTask(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	var task Task
	err := json.Unmarshal([]byte(args[0]), &task)
	if err != nil {
		fmt.Println("Error unmarshal task", err)
		return nil, err
	}
	// compute size
	// set owner

	return nil, t.saveTask(stub, &task)
}

// Update the task ID table by adding the new ID
func (t *CDNManager) updateTaskIDList(stub shim.ChaincodeStubInterface, newID string) error {
	idBytes, err := stub.GetState(TASK_IDS)
	if err != nil {
		fmt.Println("Error retrieving task IDs")
		return err
	}

	var ids []string
	err = json.Unmarshal(idBytes, &ids)
	if err != nil {
		fmt.Println("Error umarshel IDs")
		return err
	}

	if indexOf(ids, newID) == -1 {
		ids = append(ids, newID)
	}
	idsBytesToWrite, err := json.Marshal(&ids)
	if err != nil {
		fmt.Println("Error marshalling IDs")
		return errors.New("Error marshalling the IDs")
	}
	fmt.Println("Put tast IDs on TaskIDs")
	err = stub.PutState(TASK_IDS, idsBytesToWrite)
	if err != nil {
		fmt.Println("Error writting task IDs back")
		return errors.New("Error writting task IDs back")
	}

	return nil
}

func (t *CDNManager) claimTask(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	cdnNodeName := args[0]
	taskId := args[1]

	task, terr := t.getTaskById(stub, taskId)
	if terr != nil {
		return nil, terr
	}
	if indexOf(task.Nodes, cdnNodeName) == -1 {
		task.Nodes = append(task.Nodes, cdnNodeName)
	}
	t.saveTask(stub, task)

	node, nerr := t.getNodeByName(stub, cdnNodeName)
	if nerr != nil {
		return nil, nerr
	}
	if indexOf(node.Tasks, taskId) == -1 {
		node.Tasks = append(node.Tasks, taskId)
	}
	t.saveCDNNode(stub, node)
	return nil, nil
}

// Get all tasks
func (t *CDNManager) getTaskList(stub shim.ChaincodeStubInterface) ([]Task, error) {
	keysIter, err := stub.RangeQueryState(TASK_PREFIX, TASK_PREFIX+"~")
	if err != nil {
		return nil, errors.New("Unable to start the iterator")
	}

	defer keysIter.Close()

	var allTasks []Task
	for keysIter.HasNext() {
		_, taskBytes, iterErr := keysIter.Next()
		if iterErr != nil {
			return nil, fmt.Errorf("Keys operation failed. Error accessing state: %s", iterErr)
		}
		var task Task
		err = json.Unmarshal(taskBytes, &task)
		if err != nil {
			fmt.Println("Error unmarshal task", err)
			return nil, err
		}
		allTasks = append(allTasks, task)
	}

	return allTasks, nil
}

func (t *CDNManager) getReport(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	getAllTask := false
	var taskID, nodeName string
	if len(args) == 0 {
		getAllTask = true
	}
	if len(args) >= 1 {
		taskID = args[0]
	}
	if len(args) >= 2 {
		nodeName = args[1]
	}

	keysIter, err := stub.RangeQueryState(VISITED_PREFIX, VISITED_PREFIX+"~")
	if err != nil {
		return nil, errors.New("Unable to start the iterator")
	}

	defer keysIter.Close()

	var allRecord []ResouceVisitRecord
	for keysIter.HasNext() {
		_, recordBytes, iterErr := keysIter.Next()
		if iterErr != nil {
			return nil, fmt.Errorf("Keys operation failed. Error accessing state: %s", iterErr)
		}
		var record ResouceVisitRecord
		err = json.Unmarshal(recordBytes, &record)
		if err != nil {
			fmt.Println("Error unmarshal visit record", err)
			return nil, err
		}
		if getAllTask || record.TaskID == taskID || record.CDNNodeName == nodeName {
			allRecord = append(allRecord, record)
		}
	}
	return json.Marshal(&allRecord)
}

//////////////////////////////////////////////////////////// frequent operations

func indexOf(strList []string, strToFind string) int {
	for idx, s := range strList {
		if s == strToFind {
			return idx
		}
	}
	return -1
}

func (t *CDNManager) saveTask(stub shim.ChaincodeStubInterface, task *Task) error {
	if task.ID == "" {
		// Generate an UUID as task ID
		task.ID = uuid.New().String()
	}
	if task.Time == 0 {
		task.Time = time.Now().Unix()
	}
	taskBytes, err := json.Marshal(task)
	if err != nil {
		return err
	}

	err = stub.PutState(TASK_PREFIX+task.ID, taskBytes)
	if err != nil {
		return err
	}
	return err
}

func (t *CDNManager) saveCDNNode(stub shim.ChaincodeStubInterface, node *CDNNode) error {
	if node.Name == "" {
		return errors.New("Can not save a cdn node without name")
	}
	if node.IP == "" {
		return errors.New("Can not save a cdn node without ip")
	}
	nodeBytes, err := json.Marshal(&node)
	if err != nil {
		return err
	}

	err = stub.PutState(NODE_PREFIX+node.Name, nodeBytes)
	return err
}

func (t *CDNManager) saveVisitRecord(stub shim.ChaincodeStubInterface, record ResouceVisitRecord) error {
	if record.TaskID == "" {
		return errors.New("Can not save a recored without task id")
	}
	if record.CDNNodeName == "" {
		return errors.New("Can not save a record without cdn name")
	}
	if record.EndpointIP == "" {
		return errors.New("Can not save a record without endpoint IP")
	}
	if record.Time == 0 {
		// TODO: this may increase non-deterministic
		record.Time = time.Now().Unix()
	}

	recordBytes, err := json.Marshal(&record)
	if err != nil {
		return err
	}

	err = stub.PutState(VISITED_PREFIX+strconv.FormatInt(record.Time, 10), recordBytes)
	return err
}

func (t *CDNManager) getTaskById(stub shim.ChaincodeStubInterface, taskId string) (*Task, error) {
	taskBytes, err := stub.GetState(TASK_PREFIX + taskId)
	if err != nil {
		fmt.Println("Error fetching task using id:" + (TASK_PREFIX + taskId))
		return nil, err
	}
	if len(taskBytes) == 0 {
		return nil, errors.New("No task is found using id " + taskId)
	}

	var task Task
	err = json.Unmarshal(taskBytes, &task)
	if err != nil {
		fmt.Println("Error unmarshal task", err)
		return nil, err
	}
	return &task, nil
}

func (t *CDNManager) getNodeList(stub shim.ChaincodeStubInterface) ([]CDNNode, error) {
	keysIter, err := stub.RangeQueryState(NODE_PREFIX, NODE_PREFIX+"~")
	if err != nil {
		return nil, errors.New("Unable to start the iterator")
	}

	defer keysIter.Close()

	var allNodes []CDNNode
	for keysIter.HasNext() {
		_, nodeBytes, iterErr := keysIter.Next()
		if iterErr != nil {
			return nil, fmt.Errorf("Keys operation failed. Error accessing state: %s", iterErr)
		}
		var node CDNNode
		err = json.Unmarshal(nodeBytes, &node)
		if err != nil {
			fmt.Println("Error unmarshal node", err)
			return nil, err
		}
		allNodes = append(allNodes, node)
	}

	return allNodes, nil
}

func (t *CDNManager) getNodeByName(stub shim.ChaincodeStubInterface, nodeName string) (*CDNNode, error) {
	nodeBytes, err := stub.GetState(NODE_PREFIX + nodeName)
	if err != nil {
		fmt.Println("Error fetching CDN node using id:" + (NODE_PREFIX + nodeName))
		return nil, err
	}
	if len(nodeBytes) == 0 {
		return nil, errors.New("No cdn node is found using name " + nodeName)
	}

	var node CDNNode
	err = json.Unmarshal(nodeBytes, &node)
	if err != nil {
		fmt.Println("Error unmarshal CDN node", err)
		return nil, err
	}
	return &node, nil
}

// Input: Task ID
// Output: Best CDNNodeIP based on IP match
func (t *CDNManager) locateCDN(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	endpointIP := string(args[0])
	taskID := args[1]
	if taskID == "" {
		fmt.Println("Task ID should not be blank")
		return nil, errors.New("Task ID should not be blank")
	}

	task, err := t.getTaskById(stub, taskID)
	if err != nil {
		return nil, err
	}

	if len(task.Nodes) == 0 {
		return nil, errors.New("This task has not been claimed yet")
	}
	// TODO update clent and CDNNode match algorithm
	nodeIdx := int(endpointIP[0]) % len(task.Nodes)
	node, err1 := t.getNodeByName(stub, task.Nodes[nodeIdx])
	if err1 != nil {
		return nil, err1
	}
	// Should record visit here, but blockchain does not allow writting in a query
	return []byte(node.IP), nil
}

func (t *CDNManager) recordVisit(stub shim.ChaincodeStubInterface, args []string) error {
	var record ResouceVisitRecord
	err := json.Unmarshal([]byte(args[0]), &record)
	if err != nil {
		fmt.Println("Error unmarshal visit record", err)
		return err
	}
	return t.saveVisitRecord(stub, record)
}

func (t *CDNManager) confirmRecordVisit(stub shim.ChaincodeStubInterface, args []string) error {
	if len(args) != 3 {
		fmt.Println("Need 3 parameters for confirm record visit, 1st task ID, 2nd CDN Node Name, 3rd, Endpoint IP")
		return errors.New("Need 3 parameters for confirm record visit")
	}

	iTaskID := args[0]
	iCDNNodeName := args[1]
	iEndpointIP := args[2]
	allRecordsBytes, err := t.getReport(stub, []string{})
	if err != nil {
		fmt.Println("Error Get all RecordBytes in confirmRecordVisit")
		return err
	}

	var allRecords []ResouceVisitRecord
	err = json.Unmarshal(allRecordsBytes, &allRecords)
	if err != nil {
		fmt.Println("Error unmarshal allRecordsBytes in confirmRecordVisit")
		return err
	}

	for _, visitRecord := range allRecords {
		if visitRecord.TaskID == iTaskID && visitRecord.CDNNodeName == iCDNNodeName && visitRecord.EndpointIP == iEndpointIP {
			visitRecord.Ack = 1
			err = t.saveVisitRecord(stub, visitRecord)
			if err != nil {
				fmt.Println("Error updating visit record in confirmRecordVisit")
				return err
			}
			fmt.Println("Update visit record successfully")
		}
	}
	return nil
}
