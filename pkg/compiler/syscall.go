package compiler

var syscalls = map[string]map[string]string{
	"crypto": {
		"ECDsaVerify": "Neo.Crypto.ECDsaVerify",
	},
	"enumerator": {
		"Concat": "System.Enumerator.Concat",
		"Create": "System.Enumerator.Create",
		"Next":   "System.Enumerator.Next",
		"Value":  "System.Enumerator.Value",
	},
	"storage": {
		"ConvertContextToReadOnly": "System.Storage.AsReadOnly",
		"Delete":                   "System.Storage.Delete",
		"Find":                     "System.Storage.Find",
		"Get":                      "System.Storage.Get",
		"GetContext":               "System.Storage.GetContext",
		"GetReadOnlyContext":       "System.Storage.GetReadOnlyContext",
		"Put":                      "System.Storage.Put",
	},
	"runtime": {
		"GetTrigger":   "System.Runtime.GetTrigger",
		"CheckWitness": "System.Runtime.CheckWitness",
		"Notify":       "System.Runtime.Notify",
		"Log":          "System.Runtime.Log",
		"GetTime":      "System.Runtime.GetTime",
		"Serialize":    "System.Runtime.Serialize",
		"Deserialize":  "System.Runtime.Deserialize",
	},
	"blockchain": {
		"GetBlock":                "System.Blockchain.GetBlock",
		"GetContract":             "System.Blockchain.GetContract",
		"GetHeight":               "System.Blockchain.GetHeight",
		"GetTransaction":          "System.Blockchain.GetTransaction",
		"GetTransactionFromBlock": "System.Blockchain.GetTransactionFromBlock",
		"GetTransactionHeight":    "System.Blockchain.GetTransactionHeight",
	},
	"contract": {
		"Create":  "System.Contract.Create",
		"Destroy": "System.Contract.Destroy",
		"Update":  "System.Contract.Update",
	},
	"engine": {
		"GetScriptContainer":     "System.ExecutionEngine.GetScriptContainer",
		"GetCallingScriptHash":   "System.ExecutionEngine.GetCallingScriptHash",
		"GetEntryScriptHash":     "System.ExecutionEngine.GetEntryScriptHash",
		"GetExecutingScriptHash": "System.ExecutionEngine.GetExecutingScriptHash",
	},
	"iterator": {
		"Concat": "System.Iterator.Concat",
		"Create": "System.Iterator.Create",
		"Key":    "System.Iterator.Key",
		"Keys":   "System.Iterator.Keys",
		"Next":   "System.Enumerator.Next",
		"Value":  "System.Enumerator.Value",
		"Values": "System.Iterator.Values",
	},
}
