package main

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/google/uuid"
	"github.com/hyperledger/fabric/core/chaincode/shim"
)

// CDNManager example simple Chaincode implementation
type CDNManager struct {
}

var TASK_IDS = "TaskIDs"
var USER_PREFIX = "user:"

type User struct {
	ID    string   `json:"id"`
	Score string   `json:"score"`
	IP    string   `json:"ip"`
	Tasks []string `json:"tasks"`
}

var TASK_PREFIX = "task:"

type Task struct {
	ID       string   `json:"id"`
	Provider string   `json:"provider"`
	CDNnodes []string `json:"cdnNodes"`
	Size     int      `json:"size"`
	URL      string   `json:"url"`
}

func main() {
	err := shim.Start(new(CDNManager))
	if err != nil {
		fmt.Printf("Error starting Simple chaincode: %s", err)
	}
}

// Init resets all the things
func (t *CDNManager) Init(stub shim.ChaincodeStubInterface, function string, args []string) ([]byte, error) {
	if len(args) != 1 {
		return nil, errors.New("Incorrect number of arguments. Expecting 1")
	}

	// Initialize the collection of task IDs
	fmt.Println("Initialize task IDs collection")
	fmt.Println(uuid.New().String())
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

	// Initialize few tasks
	task1 := Task{ID: "001", Provider: "IBM", CDNnodes: []string{}, Size: 1000, URL: "http://www.ibm.com"}
	task2 := Task{ID: "002", Provider: "Youku", CDNnodes: []string{}, Size: 2000, URL: "http://www.youku.com"}
	task3 := Task{ID: "003", Provider: "Tudo", CDNnodes: []string{}, Size: 3000, URL: "http://www.tudo.com"}
	task4 := Task{ID: "004", Provider: "Youtube", CDNnodes: []string{}, Size: 4000, URL: "http://www.youtube.com"}
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

func (t *CDNManager) submitTask(stub shim.ChaincodeStubInterface, args []string) ([]byte, error) {
	var task Task
	err := json.Unmarshal([]byte(args[0]), &task)
	// compute size
	// set owner

	// Generate an UUID as task ID
	// id := uuid.New().String()
	id := fmt.Sprint(time.Now().Unix())
	task.ID = id
	taskBytes, err := json.Marshal(&task)

	t.updateTaskIDList(stub, id)

	err = stub.PutState(TASK_PREFIX+task.ID, taskBytes)
	if err != nil {
		return nil, err
	}
	return nil, nil
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

	ids = append(ids, TASK_PREFIX+newID)
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
		fmt.Println("Error unmarshalling task IDs")
		return nil, err
	}

	for _, value := range taskIDs {
		taskBytes, err := stub.GetState(value)

		var task Task
		err = json.Unmarshal(taskBytes, &task)
		if err != nil {
			fmt.Println("Error retrieving task " + value)
			return nil, err
		}

		fmt.Println("Appending task" + value)
		allTasks = append(allTasks, task)
	}

	return allTasks, nil
}
