package etl

import "math/big"

type Instruction struct {
	Opcode          byte
	Name            string
	Category        string
	Gas             int
	LengthOfOperand int
	Pops            int
	Pushes          int
	Fork            string
	Description     string
	OperandBytes    []byte
	Operand         string
	Address         int
	Previous        *Instruction
	Next            *Instruction
}

type InstructionList []*Instruction

func (s InstructionList) Len() int {
	return len(s)
}
func (s InstructionList) Less(i, j int) bool {
	iOperand, iOk := new(big.Int).SetString(s[i].Operand, 16)
	if !iOk {
		return true
	}
	jOperand, jOk := new(big.Int).SetString(s[j].Operand, 16)
	if !jOk {
		return false
	}
	return iOperand.Int64() < jOperand.Int64()
}
func (s InstructionList) Swap(i, j int) {
	s[i], s[j] = s[j], s[i]
}

func (i *Instruction) Size() int {
	return 1 + i.LengthOfOperand // opcode + operand
}

func (i *Instruction) Clone() *Instruction {
	return &Instruction{
		Opcode:          i.Opcode,
		Name:            i.Name,
		Category:        i.Category,
		Gas:             i.Gas,
		LengthOfOperand: i.LengthOfOperand,
		Pops:            i.Pops,
		Pushes:          i.Pushes,
		Fork:            i.Fork,
		Description:     i.Description,
	}
}

var INSTRUCTIONS = []*Instruction{

	// Stop and Arithmetic Operations
	&Instruction{Opcode: 0x00, Name: "STOP", Category: "terminate", Gas: 0, Description: "Halts execution."},
	&Instruction{Opcode: 0x01, Name: "ADD", Category: "arithmetic", Gas: 3, Description: "Addition operation."},                                      // args=[T.Value('a'), T.Value('b')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x02, Name: "MUL", Category: "arithmetic", Gas: 5, Description: "Multiplication operation."},                                // args=[T.Value('a'), T.Value('b')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x03, Name: "SUB", Category: "arithmetic", Gas: 3, Description: "Subtraction operation."},                                   // args=[T.Value('a'), T.Value('b')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x04, Name: "DIV", Category: "arithmetic", Gas: 5, Description: "Integer division operation."},                              // args=[T.Value('a'), T.Value('b')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x05, Name: "SDIV", Category: "arithmetic", Gas: 5, Description: "Signed integer"},                                          // args=[T.Value('a'), T.Value('b')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x06, Name: "MOD", Category: "arithmetic", Gas: 5, Description: "Modulo"},                                                   // args=[T.Value('a'), T.Value('b')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x07, Name: "SMOD", Category: "arithmetic", Gas: 5, Description: "Signed modulo"},                                           // args=[T.Value('a'), T.Value('b')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x08, Name: "ADDMOD", Category: "arithmetic", Gas: 8, Description: "Modulo addition operation"},                             // args=[T.Value('a'), T.Value('b'), T.Value('mod')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x09, Name: "MULMOD", Category: "arithmetic", Gas: 8, Description: "Modulo multiplication operation"},                       // args=[T.Value('a'), T.Value('b'), T.Value('mod')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x0a, Name: "EXP", Category: "arithmetic", Gas: 10, Description: "Exponential operation."},                                  // args=[T.Value('base'), T.Value('exponent')]},// returns=['result']},
	&Instruction{Opcode: 0x0b, Name: "SIGNEXTEND", Category: "arithmetic", Gas: 5, Description: "Extend length of two's complement signed integer."}, // args=[T.Value('bits'), T.Value('num')]},// returns=[T.Value('result')]},

	// Comparison & Bitwise Logic Operations
	&Instruction{Opcode: 0x10, Name: "LT", Category: "comparison", Gas: 3, Description: "Lesser-than comparison"},                                   // args=[T.Value('a'), T.Value('b')]},// returns=[T.Bool('flag')]},
	&Instruction{Opcode: 0x11, Name: "GT", Category: "comparison", Gas: 3, Description: "Greater-than comparison"},                                  // args=[T.Value('a'), T.Value('b')]},// returns=[T.Bool('flag')]},
	&Instruction{Opcode: 0x12, Name: "SLT", Category: "comparison", Gas: 3, Description: "Signed less-than comparison"},                             // args=[T.Value('a'), T.Value('b')]},// returns=[T.Bool('flag')]},
	&Instruction{Opcode: 0x13, Name: "SGT", Category: "comparison", Gas: 3, Description: "Signed greater-than comparison"},                          // args=[T.Value('a'), T.Value('b')]},// returns=[T.Bool('flag')]},
	&Instruction{Opcode: 0x14, Name: "EQ", Category: "comparison", Gas: 3, Description: "Equality  comparison"},                                     // args=[T.Value('a'), T.Value('b')]},// returns=[T.Bool('flag')]},
	&Instruction{Opcode: 0x15, Name: "ISZERO", Category: "comparison", Gas: 3, Description: "Simple not operator"},                                  // args=[T.Value('a')]},// returns=[T.Bool('flag')]},
	&Instruction{Opcode: 0x16, Name: "AND", Category: "bitwise-logic", Gas: 3, Description: "Bitwise AND operation."},                               // args=[T.Value('a'), T.Value('b')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x17, Name: "OR", Category: "bitwise-logic", Gas: 3, Description: "Bitwise OR operation."},                                 // args=[T.Value('a'), T.Value('b')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x18, Name: "XOR", Category: "bitwise-logic", Gas: 3, Description: "Bitwise XOR operation."},                               // args=[T.Value('a'), T.Value('b')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x19, Name: "NOT", Category: "bitwise-logic", Gas: 3, Description: "Bitwise NOT operation."},                               // args=[T.Value('a'), T.Value('b')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x1a, Name: "BYTE", Category: "bitwise-logic", Gas: 3, Description: "Retrieve single byte from word"},                      // args=[T.Index32('th'), T.Word('value')]},// returns=[T.Byte('byte')]},
	&Instruction{Opcode: 0x1b, Name: "SHL", Category: "bitwise-logic", Gas: 3, Fork: "constantinople", Description: "<TBD> Shift left"},             // args=[T.Index64('shift'), T.Value('value')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x1c, Name: "SHR", Category: "bitwise-logic", Gas: 3, Fork: "constantinople", Description: "<TBD> Shift Right"},            // args=[T.Index64('shift'), T.Value('value')]},// returns=[T.Value('result')]},
	&Instruction{Opcode: 0x1d, Name: "SAR", Category: "bitwise-logic", Gas: 3, Fork: "constantinople", Description: "<TBD> Shift arithmetic right"}, // args=[T.Index64('shift'), T.Value('value')]},// returns=[T.Bool('flag')]},

	// SHA3
	&Instruction{Opcode: 0x20, Name: "SHA3", Category: "cryptographic", Gas: 30, Description: "Compute Keccak-256 hash."}, // args=[T.MemOffset('offset'), T.Length('size')]},// returns=[T.Value('sha3')]},

	// Environmental Information
	&Instruction{Opcode: 0x30, Name: "ADDRESS", Category: "envinfo", Gas: 2, Description: "Get address of currently executing account."},                                                                                                     // returns=[T.Address('this.address')]},
	&Instruction{Opcode: 0x31, Name: "BALANCE", Category: "envinfo", Gas: 20, Description: "Get balance of the given account."},                                                                                                              // args=[T.Address("address")]},// returns=[T.Value("this.balance")]},
	&Instruction{Opcode: 0x32, Name: "ORIGIN", Category: "envinfo", Gas: 2, Description: "Get execution origination address."},                                                                                                               // returns=[T.Address("tx.origin")]},
	&Instruction{Opcode: 0x33, Name: "CALLER", Category: "envinfo", Gas: 2, Description: "Get caller address.This is the address of the account that is directly responsible for this execution."},                                           // returns=[T.Address("msg.sender")]},
	&Instruction{Opcode: 0x34, Name: "CALLVALUE", Category: "envinfo", Gas: 2, Description: "Get deposited value by the instruction/transaction responsible for this execution."},                                                            // returns=[T.CallValue("msg.value")]},
	&Instruction{Opcode: 0x35, Name: "CALLDATALOAD", Category: "envinfo", Gas: 3, Description: "Get input data of current environment."},                                                                                                     // args=[T.MemOffset('dataOffset')]},// returns=[T.Data("msg.data")]},
	&Instruction{Opcode: 0x36, Name: "CALLDATASIZE", Category: "envinfo", Gas: 2, Description: "Get size of input data in current environment."},                                                                                             // returns=[T.Length("msg.data.length")]},
	&Instruction{Opcode: 0x37, Name: "CALLDATACOPY", Category: "envinfo", Gas: 3, Description: "Copy input data in current environment to memory. This pertains to the input data passed with the message call instruction or transaction."}, // args=[T.MemOffset("memOffset"), T.MemOffset("dataOffset"), T.Length("length")]},
	&Instruction{Opcode: 0x38, Name: "CODESIZE", Category: "envinfo", Gas: 2, Description: "Get size of code running in current environment."},                                                                                               // returns=[T.Length("codesize")]},
	&Instruction{Opcode: 0x39, Name: "CODECOPY", Category: "envinfo", Gas: 3, Description: "Copy code running in current environment to memory."},                                                                                            // args=[T.MemOffset("memOffset"), T.MemOffset("codeOffset"), T.Length("length")]},
	&Instruction{Opcode: 0x3a, Name: "GASPRICE", Category: "envinfo", Gas: 2, Description: "Get price of gas in current environment."},                                                                                                       // returns=[T.Gas("tx.gasprice")]},
	&Instruction{Opcode: 0x3b, Name: "EXTCODESIZE", Category: "envinfo", Gas: 20, Description: "Get size of an account's code."},                                                                                                             // args=[T.Address('address')]},// returns=["extcodesize"]},
	&Instruction{Opcode: 0x3c, Name: "EXTCODECOPY", Category: "envinfo", Gas: 20, Description: "Copy an account's code to memory."},                                                                                                          // args=[T.Address("address"), T.MemOffset("memOffset"), T.MemOffset("codeOffset"), T.Length("length")]},
	&Instruction{Opcode: 0x3d, Name: "RETURNDATASIZE", Category: "envinfo", Gas: 2, Description: "Push the size of the return data buffer onto the stack."},                                                                                  // returns=["returndatasize"]},
	&Instruction{Opcode: 0x3e, Name: "RETURNDATACOPY", Category: "envinfo", Gas: 3, Description: "Copy data from the return data buffer."},                                                                                                   // args=[T.MemOffset("memOffset"), T.MemOffset("dataOffset"), T.Length("length")]},
	&Instruction{Opcode: 0x3f, Name: "EXTCODEHASH", Category: "envinfo", Gas: 400, Fork: "constantinople", Description: "<TBD> - Constantinople"},                                                                                            // args=[T.Address("address")]},

	// Block Information
	&Instruction{Opcode: 0x40, Name: "BLOCKHASH", Category: "blockinfo", Gas: 20, Description: "Get the hash of one of the 256 most recent complete blocks."}, // args=[T.Index256("num")]},// returns=["block.blockhash"]},
	&Instruction{Opcode: 0x41, Name: "COINBASE", Category: "blockinfo", Gas: 2, Description: "Get the block's beneficiary address."},                          // returns=[T.Address("block.coinbase")]},
	&Instruction{Opcode: 0x42, Name: "TIMESTAMP", Category: "blockinfo", Gas: 2, Description: "Get the block's timestamp."},                                   // returns=[T.Timestamp("block.timestamp")]},
	&Instruction{Opcode: 0x43, Name: "NUMBER", Category: "blockinfo", Gas: 2, Description: "Get the block's number."},                                         // returns=[T.Value("block.number")]},
	&Instruction{Opcode: 0x44, Name: "DIFFICULTY", Category: "blockinfo", Gas: 2, Description: "Get the block's difficulty."},                                 // returns=[T.Value("block.difficulty")]},
	&Instruction{Opcode: 0x45, Name: "GASLIMIT", Category: "blockinfo", Gas: 2, Description: "Get the block's gas limit."},                                    // returns=[T.Gas("block.gaslimit")]},

	&Instruction{Opcode: 0x46, Name: "CHAINID", Category: "blockinfo", Gas: 2, Description: "Get the chain id."},                                   // returns=[T.Gas("chain_id")]},
	&Instruction{Opcode: 0x47, Name: "SELFBALANCE", Category: "blockinfo", Gas: 5, Description: "Get own balance."},                                // returns=[T.Gas("address(this).balance")]},
	&Instruction{Opcode: 0x48, Name: "BASEFEE", Category: "blockinfo", Gas: 2, Description: "Get the value of the base fee of the current block."}, // returns=[T.Gas("block.basefee")]},

	// Stack, Memory, Storage and Flow Operations
	&Instruction{Opcode: 0x50, Name: "POP", Category: "stack", Gas: 2, Description: "Remove item from stack."}, // args=[T.Internal("//dummy")], },

	// dummy is only there to indicate that there is a pop()
	&Instruction{Opcode: 0x51, Name: "MLOAD", Category: "memory", Gas: 3, Description: "Load word from memory."},                                             // args=[T.MemOffset("offset")]},
	&Instruction{Opcode: 0x52, Name: "MSTORE", Category: "memory", Gas: 3, Description: "Save word to memory."},                                              // args=[T.MemOffset("offset"),T.Word("value")]},
	&Instruction{Opcode: 0x53, Name: "MSTORE8", Category: "memory", Gas: 3, Description: "Save byte to memory."},                                             // args=[T.MemOffset("offset"),T.Byte("value")]},
	&Instruction{Opcode: 0x54, Name: "SLOAD", Category: "storage", Gas: 50, Description: "Load word from storage."},                                          // args=[T.MemOffset("loc")]},// returns=["value"]},
	&Instruction{Opcode: 0x55, Name: "SSTORE", Category: "storage", Gas: 0, Description: "Save word to storage."},                                            // args=[T.MemOffset("loc"), T.Word("value")]},
	&Instruction{Opcode: 0x56, Name: "JUMP", Category: "controlflow", Gas: 8, Description: "Alter the program counter."},                                     // args=[T.Label("evm.pc")]},
	&Instruction{Opcode: 0x57, Name: "JUMPI", Category: "controlflow", Gas: 10, Description: "Conditionally alter the program counter."},                     // args=[T.Label("evm.pc"), T.Bool("condition")]},
	&Instruction{Opcode: 0x58, Name: "PC", Category: "info", Gas: 2, Description: "Get the value of the program counter prior to the increment."},            // returns=[T.Label("evm.pc")]},
	&Instruction{Opcode: 0x59, Name: "MSIZE", Category: "memory", Gas: 2, Description: "Get the size of active memory in bytes."},                            // returns=[T.Length("memory.length")]},
	&Instruction{Opcode: 0x5a, Name: "GAS", Category: "info", Gas: 2, Description: "Get the amount of available gas, including the corresponding reduction"}, // returns=[T.Gas("gasleft")]},
	&Instruction{Opcode: 0x5b, Name: "JUMPDEST", Category: "label", Gas: 1, Description: "Mark a valid destination for jumps."},

	// Stack Push Operations
	&Instruction{Opcode: 0x60, Name: "PUSH1", Category: "stack", Gas: 3, LengthOfOperand: 0x1, Description: "Place 1 byte item on stack."},                // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x61, Name: "PUSH2", Category: "stack", Gas: 3, LengthOfOperand: 0x2, Description: "Place 2-byte item on stack."},                // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x62, Name: "PUSH3", Category: "stack", Gas: 3, LengthOfOperand: 0x3, Description: "Place 3-byte item on stack."},                // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x63, Name: "PUSH4", Category: "stack", Gas: 3, LengthOfOperand: 0x4, Description: "Place 4-byte item on stack."},                // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x64, Name: "PUSH5", Category: "stack", Gas: 3, LengthOfOperand: 0x5, Description: "Place 5-byte item on stack."},                // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x65, Name: "PUSH6", Category: "stack", Gas: 3, LengthOfOperand: 0x6, Description: "Place 6-byte item on stack."},                // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x66, Name: "PUSH7", Category: "stack", Gas: 3, LengthOfOperand: 0x7, Description: "Place 7-byte item on stack."},                // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x67, Name: "PUSH8", Category: "stack", Gas: 3, LengthOfOperand: 0x8, Description: "Place 8-byte item on stack."},                // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x68, Name: "PUSH9", Category: "stack", Gas: 3, LengthOfOperand: 0x9, Description: "Place 9-byte item on stack."},                // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x69, Name: "PUSH10", Category: "stack", Gas: 3, LengthOfOperand: 0xa, Description: "Place 10-byte item on stack."},              // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x6a, Name: "PUSH11", Category: "stack", Gas: 3, LengthOfOperand: 0xb, Description: "Place 11-byte item on stack."},              // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x6b, Name: "PUSH12", Category: "stack", Gas: 3, LengthOfOperand: 0xc, Description: "Place 12-byte item on stack."},              // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x6c, Name: "PUSH13", Category: "stack", Gas: 3, LengthOfOperand: 0xd, Description: "Place 13-byte item on stack."},              // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x6d, Name: "PUSH14", Category: "stack", Gas: 3, LengthOfOperand: 0xe, Description: "Place 14-byte item on stack."},              // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x6e, Name: "PUSH15", Category: "stack", Gas: 3, LengthOfOperand: 0xf, Description: "Place 15-byte item on stack."},              // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x6f, Name: "PUSH16", Category: "stack", Gas: 3, LengthOfOperand: 0x10, Description: "Place 16-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x70, Name: "PUSH17", Category: "stack", Gas: 3, LengthOfOperand: 0x11, Description: "Place 17-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x71, Name: "PUSH18", Category: "stack", Gas: 3, LengthOfOperand: 0x12, Description: "Place 18-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x72, Name: "PUSH19", Category: "stack", Gas: 3, LengthOfOperand: 0x13, Description: "Place 19-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x73, Name: "PUSH20", Category: "stack", Gas: 3, LengthOfOperand: 0x14, Description: "Place 20-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x74, Name: "PUSH21", Category: "stack", Gas: 3, LengthOfOperand: 0x15, Description: "Place 21-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x75, Name: "PUSH22", Category: "stack", Gas: 3, LengthOfOperand: 0x16, Description: "Place 22-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x76, Name: "PUSH23", Category: "stack", Gas: 3, LengthOfOperand: 0x17, Description: "Place 23-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x77, Name: "PUSH24", Category: "stack", Gas: 3, LengthOfOperand: 0x18, Description: "Place 24-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x78, Name: "PUSH25", Category: "stack", Gas: 3, LengthOfOperand: 0x19, Description: "Place 25-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x79, Name: "PUSH26", Category: "stack", Gas: 3, LengthOfOperand: 0x1a, Description: "Place 26-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x7a, Name: "PUSH27", Category: "stack", Gas: 3, LengthOfOperand: 0x1b, Description: "Place 27-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x7b, Name: "PUSH28", Category: "stack", Gas: 3, LengthOfOperand: 0x1c, Description: "Place 28-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x7c, Name: "PUSH29", Category: "stack", Gas: 3, LengthOfOperand: 0x1d, Description: "Place 29-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x7d, Name: "PUSH30", Category: "stack", Gas: 3, LengthOfOperand: 0x1e, Description: "Place 30-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x7e, Name: "PUSH31", Category: "stack", Gas: 3, LengthOfOperand: 0x1f, Description: "Place 31-byte item on stack."},             // returns=[T.Value("item")]},
	&Instruction{Opcode: 0x7f, Name: "PUSH32", Category: "stack", Gas: 3, LengthOfOperand: 0x20, Description: "Place 32-byte (full word) item on stack."}, // returns=[T.Value("item")]},

	// Duplication Operations
	&Instruction{Opcode: 0x80, Name: "DUP1", Category: "stack", Gas: 3, Pops: 1, Pushes: 2, Description: "Duplicate 1st stack item."},
	&Instruction{Opcode: 0x81, Name: "DUP2", Category: "stack", Gas: 3, Pops: 2, Pushes: 3, Description: "Duplicate 2nd stack item."},
	&Instruction{Opcode: 0x82, Name: "DUP3", Category: "stack", Gas: 3, Pops: 3, Pushes: 4, Description: "Duplicate 3rd stack item."},
	&Instruction{Opcode: 0x83, Name: "DUP4", Category: "stack", Gas: 3, Pops: 4, Pushes: 5, Description: "Duplicate 4th stack item."},
	&Instruction{Opcode: 0x84, Name: "DUP5", Category: "stack", Gas: 3, Pops: 5, Pushes: 6, Description: "Duplicate 5th stack item."},
	&Instruction{Opcode: 0x85, Name: "DUP6", Category: "stack", Gas: 3, Pops: 6, Pushes: 7, Description: "Duplicate 6th stack item."},
	&Instruction{Opcode: 0x86, Name: "DUP7", Category: "stack", Gas: 3, Pops: 7, Pushes: 8, Description: "Duplicate 7th stack item."},
	&Instruction{Opcode: 0x87, Name: "DUP8", Category: "stack", Gas: 3, Pops: 8, Pushes: 9, Description: "Duplicate 8th stack item."},
	&Instruction{Opcode: 0x88, Name: "DUP9", Category: "stack", Gas: 3, Pops: 9, Pushes: 10, Description: "Duplicate 9th stack item."},
	&Instruction{Opcode: 0x89, Name: "DUP10", Category: "stack", Gas: 3, Pops: 10, Pushes: 11, Description: "Duplicate 10th stack item."},
	&Instruction{Opcode: 0x8a, Name: "DUP11", Category: "stack", Gas: 3, Pops: 11, Pushes: 12, Description: "Duplicate 11th stack item."},
	&Instruction{Opcode: 0x8b, Name: "DUP12", Category: "stack", Gas: 3, Pops: 12, Pushes: 13, Description: "Duplicate 12th stack item."},
	&Instruction{Opcode: 0x8c, Name: "DUP13", Category: "stack", Gas: 3, Pops: 13, Pushes: 14, Description: "Duplicate 13th stack item."},
	&Instruction{Opcode: 0x8d, Name: "DUP14", Category: "stack", Gas: 3, Pops: 14, Pushes: 15, Description: "Duplicate 14th stack item."},
	&Instruction{Opcode: 0x8e, Name: "DUP15", Category: "stack", Gas: 3, Pops: 15, Pushes: 16, Description: "Duplicate 15th stack item."},
	&Instruction{Opcode: 0x8f, Name: "DUP16", Category: "stack", Gas: 3, Pops: 16, Pushes: 17, Description: "Duplicate 16th stack item."},

	// Exchange Operations
	&Instruction{Opcode: 0x90, Name: "SWAP1", Category: "stack", Gas: 3, Pops: 2, Pushes: 2, Description: "Exchange 1st and 2nd stack items."},
	&Instruction{Opcode: 0x91, Name: "SWAP2", Category: "stack", Gas: 3, Pops: 3, Pushes: 3, Description: "Exchange 1st and 3rd stack items."},
	&Instruction{Opcode: 0x92, Name: "SWAP3", Category: "stack", Gas: 3, Pops: 4, Pushes: 3, Description: "Exchange 1st and 4th stack items."},
	&Instruction{Opcode: 0x93, Name: "SWAP4", Category: "stack", Gas: 3, Pops: 5, Pushes: 4, Description: "Exchange 1st and 5th stack items."},
	&Instruction{Opcode: 0x94, Name: "SWAP5", Category: "stack", Gas: 3, Pops: 6, Pushes: 5, Description: "Exchange 1st and 6th stack items."},
	&Instruction{Opcode: 0x95, Name: "SWAP6", Category: "stack", Gas: 3, Pops: 7, Pushes: 6, Description: "Exchange 1st and 7th stack items."},
	&Instruction{Opcode: 0x96, Name: "SWAP7", Category: "stack", Gas: 3, Pops: 8, Pushes: 7, Description: "Exchange 1st and 8th stack items."},
	&Instruction{Opcode: 0x97, Name: "SWAP8", Category: "stack", Gas: 3, Pops: 9, Pushes: 9, Description: "Exchange 1st and 9th stack items."},
	&Instruction{Opcode: 0x98, Name: "SWAP9", Category: "stack", Gas: 3, Pops: 10, Pushes: 10, Description: "Exchange 1st and 10th stack items."},
	&Instruction{Opcode: 0x99, Name: "SWAP10", Category: "stack", Gas: 3, Pops: 11, Pushes: 11, Description: "Exchange 1st and 11th stack items."},
	&Instruction{Opcode: 0x9a, Name: "SWAP11", Category: "stack", Gas: 3, Pops: 12, Pushes: 12, Description: "Exchange 1st and 12th stack items."},
	&Instruction{Opcode: 0x9b, Name: "SWAP12", Category: "stack", Gas: 3, Pops: 13, Pushes: 13, Description: "Exchange 1st and 13th stack items."},
	&Instruction{Opcode: 0x9c, Name: "SWAP13", Category: "stack", Gas: 3, Pops: 14, Pushes: 14, Description: "Exchange 1st and 14th stack items."},
	&Instruction{Opcode: 0x9d, Name: "SWAP14", Category: "stack", Gas: 3, Pops: 15, Pushes: 15, Description: "Exchange 1st and 15th stack items."},
	&Instruction{Opcode: 0x9e, Name: "SWAP15", Category: "stack", Gas: 3, Pops: 16, Pushes: 16, Description: "Exchange 1st and 16th stack items."},
	&Instruction{Opcode: 0x9f, Name: "SWAP16", Category: "stack", Gas: 3, Pops: 17, Pushes: 17, Description: "Exchange 1st and 17th stack items."},

	// Logging Operations
	&Instruction{Opcode: 0xa0, Name: "LOG0", Category: "event", Gas: 375, Description: "Append log record with no topics."},     // args=[T.MemOffset("start"), T.Length("size")]},
	&Instruction{Opcode: 0xa1, Name: "LOG1", Category: "event", Gas: 750, Description: "Append log record with one topic."},     // args=[T.MemOffset("start"), T.Length("size"), T.Value("topic1")]},
	&Instruction{Opcode: 0xa2, Name: "LOG2", Category: "event", Gas: 1125, Description: "Append log record with two topics."},   // args=[T.MemOffset("start"), T.Length("size"), T.Value("topic1"), T.Value("topic2")]},
	&Instruction{Opcode: 0xa3, Name: "LOG3", Category: "event", Gas: 1500, Description: "Append log record with three topics."}, // args=[T.MemOffset("start"), T.Length("size"), T.Value("topic1"), T.Value("topic2"), T.Value("topic3")]},
	&Instruction{Opcode: 0xa4, Name: "LOG4", Category: "event", Gas: 1875, Description: "Append log record with four topics."},  // args=[T.MemOffset("start"), T.Length("size"), T.Value("topic1"), T.Value("topic2"), T.Value("topic3"), T.Value("topic4")]},

	// unofficial opcodes used for parsing.
	&Instruction{Opcode: 0xb0, Name: "UNOFFICIAL_PUSH", Category: "unofficial", Description: "unofficial opcodes used for parsing."},
	&Instruction{Opcode: 0xb1, Name: "UNOFFICIAL_DUP", Category: "unofficial", Description: "unofficial opcodes used for parsing."},
	&Instruction{Opcode: 0xb2, Name: "UNOFFICIAL_SWAP", Category: "unofficial", Description: "unofficial opcodes used for parsing."},

	// System Operations
	&Instruction{Opcode: 0xf0, Name: "CREATE", Category: "system", Gas: 32000, Description: "Create a new account with associated code."},                                                                     // args=[T.CallValue("value"), T.MemOffset("offset"), T.Length("size")]},
	&Instruction{Opcode: 0xf1, Name: "CALL", Category: "system", Gas: 40, Description: "Message-call into an account."},                                                                                       // args=[T.Gas("gas"), T.Address("address"), T.CallValue("value"), T.MemOffset("inOffset"), T.Length("inSize"), T.MemOffset("retOffset"), T.Length("retSize")]},
	&Instruction{Opcode: 0xf2, Name: "CALLCODE", Category: "system", Gas: 40, Description: "Message-call into this account with alternative account's code."},                                                 // args=[T.Gas("gas"), T.Address("address"), T.CallValue("value"), T.MemOffset("inOffset"), T.Length("inSize"), T.MemOffset("retOffset"), T.Length("retSize")]},
	&Instruction{Opcode: 0xf3, Name: "RETURN", Category: "terminate", Gas: 0, Description: "Halt execution returning output data."},                                                                           // args=[T.MemOffset("offset"), T.Length("size")]},
	&Instruction{Opcode: 0xf4, Name: "DELEGATECALL", Category: "system", Gas: 40, Description: "Similar to CALLCODE except that it propagates the sender and value from the parent scope to the child scope"}, // args=[T.Gas("gas"), T.Address("address"), T.MemOffset("inOffset"), T.Length("inSize"), T.MemOffset("retOffset"), T.Length("retSize")]},
	&Instruction{Opcode: 0xf5, Name: "CREATE2", Category: "system", Gas: 32000, Fork: "constantinople", Description: "Create a new account with associated code. (Constantinople)"},                           // args=[T.Value("endowment"), T.MemOffset("offset"), T.Length("size"), T.Value("salt")]},

	// Newer opcode
	&Instruction{Opcode: 0xfa, Name: "STATICCALL", Category: "system", Gas: 40, Description: "Call another contract (or itself) while disallowing any modifications to the state during the call."}, // args=[T.Gas("gas"), T.Address("address"), T.MemOffset("inOffset"), T.Length("inSize"), T.MemOffset("retOffset"), T.Length("retSize")]},
	&Instruction{Opcode: 0xfd, Name: "REVERT", Category: "terminate", Gas: 0, Description: "throw an error"},                                                                                        // args=[T.MemOffset("offset"), T.Length("size")]},

	// Halt Execution, Mark for deletion
	&Instruction{Opcode: 0xff, Name: "SELFDESTRUCT", Category: "terminate", Gas: 0, Description: "Halt execution and register account for later deletion."}, // args=[T.Address("address")]},
}

var INSTRUCTIONMAP = make(map[byte]*Instruction)

func init() {
	for _, instruction := range INSTRUCTIONS {
		INSTRUCTIONMAP[instruction.Opcode] = instruction
	}
}
