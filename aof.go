// Package aof provides types and functions to read, parse and write redis append only file (AOF)
// to know more about redis persistence see http://oldblog.antirez.com/post/redis-persistence-demystified.html
package aof

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

var commandsWithoutKey map[string]bool
var commandsWithSubOp map[string]bool

func init() {
	commandsWithoutKey = map[string]bool{"FLUSHALL": true, "FLUSHDB": true, "SELECT": true}
	commandsWithSubOp = map[string]bool{"BITOP": true}
}

// Operation represent 1 redis operation
type Operation struct {
	Command   string
	SubOp     string
	Key       string
	Arguments []string
}

// Reader is the interface used to read an AofStream and parse 1 Operation
type Reader interface {
	ReadOperation() (Operation, error)
}

// implementation of Reader using a bufio to read
type bufioReader struct {
	input *bufio.Reader
}

// NewBufioReader creates a new bufioReader from an io.Reader
func NewBufioReader(reader io.Reader) Reader {
	input := bufio.NewReader(reader)
	return bufioReader{input: input}
}

// UnexpectedEOF is generated when a corruption is found in redis AOF, commonly an EOF or \n (delimiter)
// when more data is expected
type UnexpectedEOF struct {
	msg string
}

func (e UnexpectedEOF) Error() string {
	return "Unexpected EOF: " + e.msg
}

func (reader bufioReader) readLine() (s string, err error) {
	str, err := reader.input.ReadString('\n')
	if err != nil {
		return
	}
	str = strings.TrimSuffix(str, "\n")
	s = strings.TrimSuffix(str, "\r")
	return
}

func (reader bufioReader) readParameter() (s string, err error) {
	// read parameter length
	str, err := reader.readLine()
	if err != nil {
		return
	}
	if len(str) < 2 {
		err = UnexpectedEOF{msg: "Invalid parameter length"}
		return
	}
	if string(str[0]) != "$" {
		se := fmt.Sprintf("Corrupt File: Element is not parameter length")
		err = UnexpectedEOF{msg: se}
		return
	}
	size, e := strconv.Atoi(str[1:])
	if e != nil {
		se := fmt.Sprintf("Corrupt File: invalid number of parameters '%s' error:"+e.Error(), str[1:])
		err = UnexpectedEOF{msg: se}
		return
	}
	// read parameter
	str, err = reader.readLine()
	if err != nil {
		return
	}
	if len(str) != size {
		se := fmt.Sprintf("Corrupt File: invalid parameter length expected:%d got:%d value:'%s'", size, len(str), str)
		err = UnexpectedEOF{msg: se}
		return
	}
	s = str
	return
}

func commandHasKey(command string) bool {
	if commandsWithoutKey[strings.ToUpper(command)] {
		return false
	}
	return true
}

func commandHasSubOps(command string) bool {
	if commandsWithSubOp[strings.ToUpper(command)] {
		return true
	}
	return false
}

// ReadOperation reads one Operation from input
// returns Operation or error
func (reader bufioReader) ReadOperation() (op Operation, err error) {
	// read parameter count
	var key string
	var subop string
	str, err := reader.readLine()
	if err != nil {
		return
	}
	if len(str) < 2 {
		err = UnexpectedEOF{msg: "Invalid operation length size"}
		return
	}
	if string(str[0]) != "*" {
		se := fmt.Sprintf("Corrupt File: invalid operation parameter count")
		err = UnexpectedEOF{msg: se}
		return
	}
	count, e := strconv.Atoi(str[1:])
	if e != nil {
		se := fmt.Sprintf("Corrupt File: invalid operation parameter count '%s' error:"+e.Error(), str[1:])
		err = UnexpectedEOF{msg: se}
		return
	}
	// read command
	command, e := reader.readParameter()
	if e != nil {
		se := fmt.Sprintf("Corrupt File: Command can't be read. Error:" + e.Error())
		err = UnexpectedEOF{msg: se}
		return
	}

	if commandHasSubOps(command) {
		// read subop
		subop, e = reader.readParameter()
		if e != nil {
			se := fmt.Sprintf("Corrupt File: subop can't be read. Error:" + e.Error())
			err = UnexpectedEOF{msg: se}
			return
		}
		count-- // decrement count. as subop counts as one
	}

	if commandHasKey(command) {
		// read key
		key, e = reader.readParameter()
		if e != nil {
			se := fmt.Sprintf("Corrupt File: key can't be read. Error:" + e.Error())
			err = UnexpectedEOF{msg: se}
			return
		}
		count-- // decrement count. as key counts as one
	}

	var atts []string
	for i := 1; i < count; i++ {
		// read attributes
		att, e := reader.readParameter()
		if e != nil {
			se := fmt.Sprintf("Corrupt File: attribute pos:%d can't be read. Error:"+e.Error(), i)
			err = UnexpectedEOF{msg: se}
			return
		}
		atts = append(atts, att)
	}

	op.Command = command
	op.SubOp = subop
	op.Key = key
	op.Arguments = atts
	return
}

func writeString2(str, msg string, out io.Writer) (err error) {
	n, err := out.Write([]byte(str))
	if err == nil {
		if n != len(str) {
			err = fmt.Errorf("error writing %s. Written %d bytes expected %d", msg, n, len(str))
		}
	}
	return
}

func writeString(str string, out io.Writer) (err error) {
	size := len(str)
	s := fmt.Sprintf("$%d\r\n", size)
	err = writeString2(s, "string length", out)
	if err == nil {
		s = fmt.Sprintf("%s\r\n", str)
		err = writeString2(s, "string value", out)
	}
	return
}

//ToAof generates the AOF representation of the Operation and write it to out
//returns error or nil in case of success
func (op Operation) ToAof(out io.Writer) (err error) {
	// write parameter count
	paramCount := 1 // 1 for command
	if commandHasKey(op.Command) {
		paramCount++ // count key
	}
	if commandHasSubOps(op.Command) {
		paramCount++ // count subop
	}
	paramCount += len(op.Arguments)
	s := fmt.Sprintf("*%d\r\n", paramCount)
	n, err := out.Write([]byte(s))
	if err != nil {
		return
	}
	if n != len(s) {
		err = fmt.Errorf("error writing length written %d expected %d", n, len(s))
		return
	}
	//write command
	err = writeString(op.Command, out)
	if err != nil {
		return
	}
	// write subop
	if commandHasSubOps(op.Command) {
		err = writeString(op.SubOp, out)
		if err != nil {
			return
		}
	}
	// write key
	if commandHasKey(op.Command) {
		err = writeString(op.Key, out)
		if err != nil {
			return
		}
	}
	// write attributes
	for i := 0; i < len(op.Arguments); i++ {
		err = writeString(op.Arguments[i], out)
		if err != nil {
			return
		}
	}
	return
}
