package main

import (
	"encoding/json"
	"errors"
	"fmt"

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
	Score string   `json:"score"`
	IP    string   `json:"ip"`
	Tasks []string `json:"tasks"`
}

var TASK_PREFIX = "task:"

type Task struct {
	ID       string   `json:"id"`
	Customer string   `json:"customer"`
	Nodes    []string `json:"nodes"`
	Size     string   `json:"size"`
	Type     string   `json:"type"`
	URL      string   `json:"url"`
}

func main() {
	err := shim.Start(new(CDNManager))
	if err != nil {
		fmt.Printf("Error starting Simple chaincode:", err)
	}
}

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

	/*
	 */
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
		return nil, err1
	}
	if err2 != nil {
		fmt.Println("Error Marshal task2")
		return nil, err2
	}
	if err3 != nil {
		fmt.Println("Error Marshal task3")
		return nil, err3
	}
	if err4 != nil {
		fmt.Println("Error Marshal task4")
		return nil, err4
	}
	err1 = stub.PutState(TASK_PREFIX+task1.ID, task1Bytes)
	t.updateTaskIDList(stub, task1.ID)
	err2 = stub.PutState(TASK_PREFIX+task2.ID, task2Bytes)
	t.updateTaskIDList(stub, task2.ID)
	err3 = stub.PutState(TASK_PREFIX+task3.ID, task3Bytes)
	t.updateTaskIDList(stub, task3.ID)
	err4 = stub.PutState(TASK_PREFIX+task4.ID, task4Bytes)
	t.updateTaskIDList(stub, task4.ID)

	fmt.Println("Initialization complete")
	return nil, nil
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
		} else {
			taskListBytes, err1 := json.Marshal(&taskList)
			if err1 != nil {
				fmt.Println("Error marshalling taskList")
				return nil, err1
			}
			fmt.Println("All success, returning taskList")
			return taskListBytes, nil
		}
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

	err = t.saveTask(stub, &task)
	if err != nil {
		fmt.Println("Error saving task", err)
		return nil, err
	}
	return nil, t.updateTaskIDList(stub, task.ID)
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

	// TODO: check existence
	ids = append(ids, newID)
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
	task.Nodes = append(task.Nodes, cdnNodeName)
	t.saveTask(stub, task)

	node, nerr := t.getNodeByName(stub, cdnNodeName)
	if nerr != nil {
		return nil, nerr
	}
	node.Tasks = append(node.Tasks, taskId)
	t.saveCDNNode(stub, node)
	return nil, nil
}

// Get all tasks
func (t *CDNManager) getTaskList(stub shim.ChaincodeStubInterface) ([]Task, error) {
	var allTasks []Task

	taskIDBytes, err := stub.GetState(TASK_IDS)
	if err != nil {
		fmt.Println("Error retrieving task IDs")
		return nil, err
	}
	var taskIDs []string
	err = json.Unmarshal(taskIDBytes, &taskIDs)
	if err != nil {
		fmt.Println("Error unmarshalling task IDs", err)
		return nil, err
	}

	for _, taskId := range taskIDs {
		task, err := t.getTaskById(stub, taskId)
		if err != nil {
			fmt.Println("Error getting task  by ID(", taskId, ")", err)
			return nil, err
		}

		fmt.Println("Appending task" + taskId)
		allTasks = append(allTasks, *task)
	}

	return allTasks, nil
}

//////////////////////////////////////////////////////////// frequent operations

func (t *CDNManager) saveTask(stub shim.ChaincodeStubInterface, task *Task) error {
	if task.ID == "" {
		// Generate an UUID as task ID
		task.ID = uuid.New().String()
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
