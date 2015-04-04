package aof

import (
	"bufio"
	"io"
	"os"
	"testing"
)

func TestRead(t *testing.T) {
	file, err := os.Open("test-data.aof")
	if err != nil {
		t.Errorf("Can't open file. Error:'%s'", err.Error())
		return
	}
	input := bufio.NewReader(file)
	op1, err := ReadOperation(input)
	if err != nil {
		t.Errorf("Error reading operation 1 :'%s'", err.Error())
		return
	}
	if op1.Command != "SELECT" {
		t.Errorf("Wrong command '%s' expected 'SELECT'", op1.Command)
		return
	}
	op2, err := ReadOperation(input)
	if err != nil {
		t.Errorf("Error reading operation 2 :'%s'", err.Error())
		return
	}
	if op2.Command != "SET" {
		t.Errorf("Wrong command '%s' expected 'SET'", op1.Command)
		return
	}
	op3, err := ReadOperation(input)
	if err != nil {
		t.Errorf("Error reading operation 3 :'%s'", err.Error())
		return
	}
	if op3.Command != "SADD" {
		t.Errorf("Wrong command '%s' expected 'SADD'", op1.Command)
		return
	}
	op4, err := ReadOperation(input)
	if err != nil {
		t.Errorf("Error reading operation 4 :'%s'", err.Error())
		return
	}
	if op4.Command != "SADD" {
		t.Errorf("Wrong command '%s' expected 'SADD'", op1.Command)
		return
	}
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("An error was expected but got nil")
		return
	}
	if err != io.EOF {
		t.Errorf("Wrong error '%s' expected '%s'", err.Error(), io.EOF.Error())
		return
	}

}
func TestUnexpectedEofNoArguments(t *testing.T) {
	file, err := os.Open("test-data-eof1.aof")
	if err != nil {
		t.Errorf("Can't open file. Error:'%s'", err.Error())
		return
	}
	input := bufio.NewReader(file)
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("An error was expected but got nil")
		return
	}
	_, ok := err.(UnexpectedEOF)
	if !ok {
		t.Errorf("Wrong error '%s' expected 'UnexpectedEOF'", err.Error())
		return
	}
}
func TestUnexpectedEofInvalidNumberOfArguments(t *testing.T) {
	file, err := os.Open("test-data-eof2.aof")
	if err != nil {
		t.Errorf("Can't open file. Error:'%s'", err.Error())
		return
	}
	input := bufio.NewReader(file)
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("An error was expected but got nil")
		return
	}
	_, ok := err.(UnexpectedEOF)
	if !ok {
		t.Errorf("Wrong error '%s' expected 'UnexpectedEOF'", err.Error())
		return
	}
}
func TestUnexpectedEofInvalidCommandSize(t *testing.T) {
	file, err := os.Open("test-data-eof3.aof")
	if err != nil {
		t.Errorf("Can't open file. Error:'%s'", err.Error())
		return
	}
	input := bufio.NewReader(file)
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("An error was expected but got nil")
		return
	}
	_, ok := err.(UnexpectedEOF)
	if !ok {
		t.Errorf("Wrong error '%s' expected 'UnexpectedEOF'", err.Error())
		return
	}
}
func TestUnexpectedEofInvalidArgumentSize(t *testing.T) {
	file, err := os.Open("test-data-eof4.aof")
	if err != nil {
		t.Errorf("Can't open file. Error:'%s'", err.Error())
		return
	}
	input := bufio.NewReader(file)
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("An error was expected but got nil")
		return
	}
	_, ok := err.(UnexpectedEOF)
	if !ok {
		t.Errorf("Wrong error '%s' expected 'UnexpectedEOF'", err.Error())
		return
	}
}

func TestFlushallSupport(t *testing.T) {
	file, err := os.Open("test-data-flushall.aof")
	if err != nil {
		t.Errorf("Can't open file. Error:'%s'", err.Error())
		return
	}
	input := bufio.NewReader(file)
	op1, err := ReadOperation(input)
	if err != nil {
		t.Errorf("Error reading operation 1 :'%s'", err.Error())
		return
	}
	if op1.Command != "FLUSHALL" {
		t.Errorf("Wrong command '%s' expected 'FLUSHALL'", op1.Command)
		return
	}
	if len(op1.Arguments) != 0 {
		t.Errorf("Wrong argument count '%d' expected '0'.", len(op1.Arguments))
		return
	}
	_, err = ReadOperation(input)
	if err != nil {
		t.Errorf("Error reading operation 2 :'%s'", err.Error())
		return
	}
}

func TestFlushdbSupport(t *testing.T) {
	file, err := os.Open("test-data-flushdb.aof")
	if err != nil {
		t.Errorf("Can't open file. Error:'%s'", err.Error())
		return
	}
	input := bufio.NewReader(file)
	op1, err := ReadOperation(input)
	if err != nil {
		t.Errorf("Error reading operation 1 :'%s'", err.Error())
		return
	}
	if op1.Command != "FLUSHDB" {
		t.Errorf("Wrong command '%s' expected 'FLUSHDB'", op1.Command)
		return
	}
	if len(op1.Arguments) != 0 {
		t.Errorf("Wrong argument count '%d' expected '0'.", len(op1.Arguments))
		return
	}
	_, err = ReadOperation(input)
	if err != nil {
		t.Errorf("Error reading operation 2 :'%s'", err.Error())
		return
	}
}

func TestBitopSupport(t *testing.T) {
	file, err := os.Open("test-data-bitop.aof")
	if err != nil {
		t.Errorf("Can't open file. Error:'%s'", err.Error())
		return
	}
	input := bufio.NewReader(file)
	op1, err := ReadOperation(input)
	if err != nil {
		t.Errorf("Error reading operation 1 :'%s'", err.Error())
		return
	}
	if op1.Command != "bitop" {
		t.Errorf("Wrong command '%s' expected 'bitop'", op1.Command)
		return
	}
	if op1.SubOp != "xor" {
		t.Errorf("Wrong subop '%s' expected 'xor'", op1.SubOp)
		return
	}
	if op1.Key != "k3" {
		t.Errorf("Wrong key '%s' expected 'k3'", op1.Key)
		return
	}

	if len(op1.Arguments) != 2 {
		t.Errorf("Wrong argument count '%d' expected '2'.", len(op1.Arguments))
		return
	}
	_, err = ReadOperation(input)
	if err != nil {
		t.Errorf("Error reading operation 2 :'%s'", err.Error())
		return
	}
}
