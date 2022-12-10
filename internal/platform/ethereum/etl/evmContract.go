package etl

import (
	"encoding/hex"
	"fmt"
	"strings"
	"time"
)

type EvmContract struct {
	EnableStaticAnalysis  bool
	EnableDynamicAnalysis bool
	Bytecode              []byte
	Instructions          []*Instruction
	InstructionAt         map[int]*Instruction
	First                 *Instruction
	Last                  *Instruction
	Jumptable             map[int]*Instruction
	StringsAt             map[int]*Instruction
	Duration              int64
	Push4Instructions     []*Instruction
	FunctionSighasheMap   map[string]string
}

func (c *EvmContract) SetHexcode(hexcode string) bool {
	if strings.HasPrefix(hexcode, "0x") {
		hexcode = hexcode[2:]
	}
	if len(hexcode)&1 == 1 {
		hexcode = "0" + hexcode
	}
	byteValue, err := hex.DecodeString(hexcode)
	if err != nil {
		return false
	}
	c.Bytecode = byteValue
	return true
}

func (c *EvmContract) SimpleInitFunctionSighashes() bool {
	if len(c.Bytecode) == 0 {
		return false
	}

	disassemble := c.simpleDisassemble()
	if !disassemble {
		return false
	}

	instructions := c.Instructions
	var functionSighasheMap = make(map[string]string)
	for _, instruction := range instructions {
		if instruction.Name == "PUSH4" || instruction.Name == "PUSH3" {
			operand := instruction.Operand
			functionSighasheMap[operand] = ""
		}
	}
	c.FunctionSighasheMap = functionSighasheMap

	return true
}

func (c *EvmContract) simpleDisassemble() bool {
	if len(c.Bytecode) == 0 {
		return false
	}

	tStart := time.Now()
	c.getInstructions()
	if len(c.Instructions) == 0 {
		return false
	}

	// timekeeping
	c.Duration = time.Since(tStart).Microseconds()

	return true
}

func (c *EvmContract) getInstructions() {
	bytecode := c.Bytecode
	bytecodeLen := len(bytecode)

	var pc = 0
	var previous *Instruction

	for pc < bytecodeLen {
		opcode := bytecode[pc]
		var instruction = findByOpcode(opcode)
		if instruction != nil {
			var operandLen = instruction.LengthOfOperand
			stopIndex := pc + operandLen + 1
			if stopIndex > bytecodeLen {
				stopIndex = bytecodeLen
			}
			var operandBytes = bytecode[pc+1 : stopIndex]
			instruction.OperandBytes = operandBytes
			operand := hex.EncodeToString(operandBytes)
			instruction.Operand = operand
			if len(instruction.OperandBytes) != instruction.LengthOfOperand {
				fmt.Println("invalid instruction: ", instruction.Name)
				instruction.Name = "INVALID_" + hex.EncodeToString([]byte{opcode})
				instruction.Description = "Invalid operand"
				instruction.Category = "unknown"
			}
		} else {
			instruction = &Instruction{Opcode: opcode,
				Name:        "UNKNOWN_" + hex.EncodeToString([]byte{opcode}),
				Description: "Invalid opcode",
				Category:    "unknown"}
		}

		instruction.Address = pc
		pc += instruction.Size()
		// doubly link
		instruction.Previous = previous
		if previous != nil {
			previous.Next = instruction
		}
		// current is previous
		previous = instruction

		c.Instructions = append(c.Instructions, instruction)
	}
}

func findByOpcode(opcode byte) *Instruction {
	instruction := INSTRUCTIONMAP[opcode]
	if instruction != nil {
		return instruction.Clone()
	}
	return nil
}

func (c *EvmContract) implements(functionSighashe string) bool {
	_, ok := c.FunctionSighasheMap[functionSighashe]
	return ok
}

func (c *EvmContract) implementsAnyOf(functionSighashes []string) bool {
	for _, functionSighashe := range functionSighashes {
		_, ok := c.FunctionSighasheMap[functionSighashe]
		if ok {
			return true
		}
	}
	return false
}

// https://github.com/ethereum/EIPs/blob/master/EIPS/eip-20.md
// https://github.com/OpenZeppelin/openzeppelin-solidity/blob/master/contracts/token/ERC20/ERC20.sol
func (c *EvmContract) IsErc20Contract() bool {
	/*return c.implements("totalSupply()") &&
	c.implements("balanceOf(address)") &&
	c.implements("transfer(address,uint256)") &&
	c.implements("transferFrom(address,address,uint256)") &&
	c.implements("approve(address,uint256)") &&
	c.implements("allowance(address,address)")*/
	return c.implements("18160ddd") &&
		c.implements("70a08231") &&
		c.implements("a9059cbb") &&
		c.implements("23b872dd") &&
		c.implements("095ea7b3") &&
		c.implements("dd62ed3e")
}

// https://github.com/ethereum/EIPs/blob/master/EIPS/eip-721.md
// https://github.com/OpenZeppelin/openzeppelin-solidity/blob/master/contracts/token/ERC721/ERC721Basic.sol
// Doesn't check the below ERC721 methods to match CryptoKitties contract
// getApproved(uint256)
// setApprovalForAll(address,bool)
// isApprovedForAll(address,address)
// transferFrom(address,address,uint256)
// safeTransferFrom(address,address,uint256)
// safeTransferFrom(address,address,uint256,bytes)
func (c *EvmContract) IsErc721Contract() bool {
	/*return c.implements("balanceOf(address)") &&
	c.implements("ownerOf(uint256)") &&
	c.implementsAnyOf([]string{"transfer(address,uint256)", "transferFrom(address,address,uint256)"}) &&
	c.implements("approve(address,uint256)")*/
	return c.implements("70a08231") &&
		c.implements("6352211e") &&
		c.implementsAnyOf([]string{"a9059cbb", "23b872dd"}) &&
		c.implements("095ea7b3")
}

// https://github.com/ethereum/EIPs/blob/master/EIPS/eip-1155.md
// https://github.com/OpenZeppelin/openzeppelin-solidity/blob/master/contracts/token/ERC1155/ERC1155Basic.sol
// Doesn't check the below ERC1155 methods to match old contract
// balanceOfBatch(address[],uint256[])
func (c *EvmContract) IsErc1155Contract() bool {
	/*return c.implements("balanceOf(address,uint256)") &&
	//c.implements("balanceOfBatch(address[],uint256[])") &&
	c.implements("safeTransferFrom(address,address,uint256,uint256,bytes)") &&
	c.implements("safeBatchTransferFrom(address,address,uint256[],uint256[],bytes)") &&
	c.implements("setApprovalForAll(address,bool)") &&
	c.implements("isApprovedForAll(address,address)")*/
	return c.implementsAnyOf([]string{"fdd58e", "00fdd58e"}) &&
		//c.implements("4e1273f4") &&
		c.implements("f242432a") &&
		c.implements("2eb2c2d6") &&
		c.implements("a22cb465") &&
		c.implements("e985e9c5")
}
