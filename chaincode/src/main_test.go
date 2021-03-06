/*
Copyright IBM Corp. 2016 All Rights Reserved.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

         http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/
package main

import (
	"fmt"
	"testing"

	"github.com/hyperledger/fabric/core/chaincode/shim"
)

/*
func checkInit(t *testing.T, stub *shim.MockStub, args []string) {
	_, err := stub.MockInit("1", "init", args)
	if err != nil {
		fmt.Println("Init failed", err)
		t.FailNow()
	}
}

func checkState(t *testing.T, stub *shim.MockStub, name string, value string) {
	bytes := stub.State[name]
	if bytes == nil {
		fmt.Println("State", name, "failed to get value")
		t.FailNow()
	}
	if string(bytes) != value {
		fmt.Println("State value", name, "was not", value, "as expected")
		t.FailNow()
	}
}

func checkQuery(t *testing.T, stub *shim.MockStub, name string, value string) {
	bytes, err := stub.MockQuery("query", []string{name})
	if err != nil {
		fmt.Println("Query", name, "failed", err)
		t.FailNow()
	}
	if bytes == nil {
		fmt.Println("Query", name, "failed to get value")
		t.FailNow()
	}
	if string(bytes) != value {
		fmt.Println("Query value", name, "was not", value, "as expected")
		t.FailNow()
	}
}

*/
func checkInvoke(t *testing.T, stub *shim.MockStub, funcion string, args []string) {
	_, err := stub.MockInvoke("1", funcion, args)
	if err != nil {
		fmt.Println("Invoke", args, "failed", err)
		t.FailNow()
	}
}

func checkInit(t *testing.T, stub *shim.MockStub, args []string) {
	_, err := stub.MockInit("1", "init", args)
	fmt.Println("check init")
	if err != nil {
		fmt.Println("Init failed", err)
		t.FailNow()
	}
}

func checkQuery(t *testing.T, stub *shim.MockStub, function string, args []string) {
	bytes, err := stub.MockQuery(function, args)
	if err != nil {
		fmt.Println("getTaskList", "failed", err)
		t.FailNow()
	} else if bytes == nil {
		fmt.Println("getTaskList", "failed to get value")
		t.FailNow()
	} else {
		// fmt.Println("Query did not fail as expected (PutState within Query)!", string(bytes), err)
		// t.FailNow()
		t.Log("Query returned", string(bytes))
	}
}

func Test_Init(t *testing.T) {
	chaincode := new(CDNManager)
	stub := shim.NewMockStub("cdn-manager", chaincode)

	checkInit(t, stub, []string{"A"})
	fmt.Println("Test Init sucess")
}

// func Test_submitTask(t *testing.T) {
// 	chaincode := new(CDNManager)
// 	stub := shim.NewMockStub("cdn-manager", chaincode)

// 	checkInit(t, stub, []string{"A"})

// 	checkInvoke(t, stub, "submitTask", []string{`{"size": "999", "url": "http://www.ibm.com"}`})

// 	checkQuery(t, stub, "getTaskList", []string{})
// }

/* func Test_claimTask(t *testing.T) {
	chaincode := new(CDNManager)
	stub := shim.NewMockStub("cdn-manager", chaincode)

	checkInit(t, stub, []string{"A"})

	checkInvoke(t, stub, "submitTask", []string{`{"id": "task-uuid", "url": "http://www.ibm.com"}`})
	checkInvoke(t, stub, "registerCDNNode", []string{`{"name": "cdnName1", "ip": "1.2.3.4"}`})
	checkInvoke(t, stub, "registerCDNNode", []string{`{"name": "cdnName2", "ip": "cdn.mybluemix.net"}`})
	checkQuery(t, stub, "getTaskList", []string{})
	// checkQuery(t, stub, "getNodeList", []string{})
	// checkInvoke(t, stub, "claimTask", []string{"cdnName1", "task-uuid"})
	// checkInvoke(t, stub, "claimTask", []string{"cdnName2", "task-uuid"})
	// checkQuery(t, stub,)
	// checkQuery(t, stub, "locateCDN", []string{"9.12.34.56", "task-uuid"})
	// checkQuery(t, stub, "locateCDN", []string{"8.12.34.56", "task-uuid"})

	// test record visit and get report
	// checkInvoke(t, stub, "recordVisit", []string{`{"time" : 0, "taskID": "task-uuid", "cdnNodeName" : "cdnName1", "endpointIP" : "9.12.34.56", "size" : 10, "ack": 0}`})
	// checkQuery(t, stub, "getReport", []string{})
} */

func Test_ackReport(t *testing.T) {
	chaincode := new(CDNManager)
	stub := shim.NewMockStub("cdn-manager", chaincode)

	checkInit(t, stub, []string{"A"})

	checkInvoke(t, stub, "recordVisit", []string{`{"time" : 0, "taskID": "task-uuid", "cdnNodeName" : "cdnName1", "endpointIP" : "9.12.34.56", "size" : 10, "ack": 0}`})
	checkQuery(t, stub, "getReport", []string{})
	checkInvoke(t, stub, "confirmRecordVisit", []string{"task-uuid", "cdnName1", "9.12.34.56"})
	checkQuery(t, stub, "getReport", []string{})
}
