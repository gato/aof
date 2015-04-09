package aof

import (
	"bufio"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"
)

func TestRead(t *testing.T) {
	file, err := os.Open("test-data.aof")
	defer file.Close()
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
		t.Errorf("An error was expected")
		return
	}
	if err != io.EOF {
		t.Errorf("Wrong error '%s' expected '%s'", err.Error(), io.EOF.Error())
		return
	}

}
func TestUnexpectedEofNoArguments(t *testing.T) {
	file, err := os.Open("test-data-eof1.aof")
	defer file.Close()
	if err != nil {
		t.Errorf("Can't open file. Error:'%s'", err.Error())
		return
	}
	input := bufio.NewReader(file)
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("An error was expected")
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
	defer file.Close()
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
	defer file.Close()
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
	defer file.Close()
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
func TestUnexpectedEofInvalidSubopSize(t *testing.T) {
	file, err := os.Open("test-data-eof5.aof")
	defer file.Close()
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
	defer file.Close()
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
	defer file.Close()
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
	defer file.Close()
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

type RecordWriter []byte

func (this *RecordWriter) Write(b []byte) (int, error) {
	*this = append(*this, b...)
	return len(b), nil
}

func TestWriteStringOk(t *testing.T) {
	var rw RecordWriter = make([]byte, 0)
	s := "hello world!"
	err := writeString(s, &rw)
	if err != nil {
		t.Errorf("Error writing string:'%s'", err.Error())
		return
	}
	expected := "$12\r\nhello world!\r\n"
	if expected != string(rw) {
		t.Errorf("Invalid written string:'%s' expected:'%s'", string(rw), expected)
		return
	}
}

type ErrorNWriter struct {
	current int
	failing int
}

func (this *ErrorNWriter) Write(b []byte) (int, error) {
	this.current += 1
	if this.current == this.failing {
		return len(b), fmt.Errorf("Some error")
	}
	return len(b), nil
}

func newErrorNWriter(failing int) ErrorNWriter {
	return ErrorNWriter{current: 0, failing: failing}
}

type TruncateNWriter struct {
	current int
	failing int
}

func (this *TruncateNWriter) Write(b []byte) (int, error) {
	this.current += 1
	if this.current == this.failing {
		return 0, nil
	}
	return len(b), nil
}

func newTruncateNWriter(failing int) TruncateNWriter {
	return TruncateNWriter{current: 0, failing: failing}
}

func TestWriteErrors(t *testing.T) {
	var ew ErrorNWriter = newErrorNWriter(1)
	s := "hello world!"
	err := writeString(s, &ew)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}

	ew = newErrorNWriter(2)
	err = writeString(s, &ew)
	if err == nil {
		t.Errorf("Error was expected but was nil")
		return
	}
}

func TestWriteTruncateErrors(t *testing.T) {
	var tw TruncateNWriter = newTruncateNWriter(1)
	s := "hello world!"
	err := writeString(s, &tw)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	if err.Error() != "Error writing string length. Written 0 bytes expected 5" {
		t.Errorf("Invalid error got '%s' expected 'Error writing string length. Written 0 bytes expected 5'", err.Error())
		return
	}

	tw = newTruncateNWriter(2)
	err = writeString(s, &tw)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	if err.Error() != "Error writing string value. Written 0 bytes expected 14" {
		t.Errorf("Invalid error got '%s' expected 'Error writing string length. Written 0 bytes expected 14'", err.Error())
		return
	}
}

func TestToAofWithoutKey(t *testing.T) {
	var op Operation = Operation{}
	op.Command = "SELECT"
	op.Arguments = append(make([]string, 0), "0")
	var rw RecordWriter = make([]byte, 0)
	err := op.ToAof(&rw)
	if err != nil {
		t.Errorf("ToAof failed, error:'%s'", err.Error())
		return
	}
	expected := "*2\r\n$6\r\nSELECT\r\n$1\r\n0\r\n"
	if string(rw) != expected {
		t.Errorf("invalid serialization got:\n%s\n expected:\n%s\n", string(rw), expected)
	}
}

func TestToAofOperationWithKey(t *testing.T) {
	var op Operation = Operation{}
	op.Command = "SADD"
	op.Key = "k1"
	op.Arguments = append(make([]string, 0), "k2", "k3")
	var rw RecordWriter = make([]byte, 0)
	err := op.ToAof(&rw)
	if err != nil {
		t.Errorf("ToAof failed, error:'%s'", err.Error())
		return
	}
	expected := "*4\r\n$4\r\nSADD\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n"
	if string(rw) != expected {
		t.Errorf("invalid serialization got:\n%s\n expected:\n%s\n", string(rw), expected)
	}
}

func TestToAofOperationWithSubOp(t *testing.T) {
	var op Operation = Operation{}
	op.Command = "BITOP"
	op.SubOp = "AND"
	op.Key = "k1"
	op.Arguments = append(make([]string, 0), "k2", "k3")
	var rw RecordWriter = make([]byte, 0)
	err := op.ToAof(&rw)
	if err != nil {
		t.Errorf("ToAof failed, error:'%s'", err.Error())
		return
	}
	expected := "*5\r\n$5\r\nBITOP\r\n$3\r\nAND\r\n$2\r\nk1\r\n$2\r\nk2\r\n$2\r\nk3\r\n"
	if string(rw) != expected {
		t.Errorf("invalid serialization got:\n%s\n expected:\n%s\n", string(rw), expected)
	}
}

func TestToAofErrors(t *testing.T) {
	var op Operation = Operation{}
	op.Command = "BITOP"
	op.SubOp = "AND"
	op.Key = "k1"
	op.Arguments = append(make([]string, 0), "k2", "k3")
	tw := newTruncateNWriter(1)
	err := op.ToAof(&tw)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	// generating the AOF for this command call write([]byte) 11 times by calling
	// writeString (except the first call which is direct)
	// This loop generates error every 2 calls to simulate failing in every call
	for i := 1; i < 12; i += 2 {
		ew := newErrorNWriter(i)
		err := op.ToAof(&ew)
		if err == nil {
			t.Errorf("Error was expected %d", i)
			return
		}

	}

}

func TestReadParameterErrors(t *testing.T) {
	input := bufio.NewReader(strings.NewReader("a"))
	_, err := readParameter(input)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	if err != io.EOF {
		t.Errorf("Wrong error '%s' expected 'EOF'", err.Error())
		return
	}

	input = bufio.NewReader(strings.NewReader("a\r\n"))
	_, err = readParameter(input)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	_, ok := err.(UnexpectedEOF)
	if !ok {
		t.Errorf("Wrong error '%s' expected 'UnexpectedEOF'", err.Error())
		return
	}

	input = bufio.NewReader(strings.NewReader("a23\r\n"))
	_, err = readParameter(input)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	_, ok = err.(UnexpectedEOF)
	if !ok {
		t.Errorf("Wrong error '%s' expected 'UnexpectedEOF'", err.Error())
		return
	}

	input = bufio.NewReader(strings.NewReader("$A\r\n"))
	_, err = readParameter(input)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	_, ok = err.(UnexpectedEOF)
	if !ok {
		t.Errorf("Wrong error '%s' expected 'UnexpectedEOF'", err.Error())
		return
	}
	input = bufio.NewReader(strings.NewReader("$6\r\nBAD"))
	_, err = readParameter(input)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	if err != io.EOF {
		t.Errorf("Wrong error '%s' expected 'EOF'", err.Error())
		return
	}
	input = bufio.NewReader(strings.NewReader("$6\r\nBAD\r\n"))
	_, err = readParameter(input)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	_, ok = err.(UnexpectedEOF)
	if !ok {
		t.Errorf("Wrong error '%s' expected 'UnexpectedEOF'", err.Error())
		return
	}

}

func TestReadOperationErrors(t *testing.T) {
	input := bufio.NewReader(strings.NewReader("a"))
	_, err := ReadOperation(input)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	if err != io.EOF {
		t.Errorf("Wrong error '%s' expected 'EOF'", err.Error())
		return
	}
	input = bufio.NewReader(strings.NewReader("a\r\n"))
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	_, ok := err.(UnexpectedEOF)
	if !ok {
		t.Errorf("Wrong error '%s' expected 'UnexpectedEOF'", err.Error())
		return
	}
	input = bufio.NewReader(strings.NewReader("a23\r\n"))
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	_, ok = err.(UnexpectedEOF)
	if !ok {
		t.Errorf("Wrong error '%s' expected 'UnexpectedEOF'", err.Error())
		return
	}

	input = bufio.NewReader(strings.NewReader("*A\r\n"))
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("Error was expected")
		return
	}
	_, ok = err.(UnexpectedEOF)
	if !ok {
		t.Errorf("Wrong error '%s' expected 'UnexpectedEOF'", err.Error())
		return
	}
}
