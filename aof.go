package aoflib

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
)

type Operation struct {
	Command   string
	Key       string
	Arguments []string
}

type UnexpectedEOF struct {
	msg string
}

func (e UnexpectedEOF) Error() string {
	return "Unexpected EOF: " + e.msg
}

func readLine(input *bufio.Reader) (s string, err error) {
	str, err := input.ReadString('\n')
	str = strings.Replace(str, "\n", "", 1)
	s = strings.Replace(str, "\r", "", 1)
	return
}

func readParameter(input *bufio.Reader) (s string, err error) {
	// read parameter length
	str, err := readLine(input)
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
	size, e := strconv.Atoi(str[1:len(str)])
	if e != nil {
		se := fmt.Sprintf("Corrupt File: invalid number of parameters '%s' error:"+e.Error(), str[1:len(str)])
		err = UnexpectedEOF{msg: se}
		return
	}
	// leo el parametro
	str, err = readLine(input)
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
	if command == "SELECT" || command == "FLUSHDB" || command == "FLUSHALL" {
		return false
	}
	return true
}

func ReadOperation(input *bufio.Reader) (op Operation, err error) {
	// read parameter count
	var key string
	str, err := readLine(input)
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
	count, e := strconv.Atoi(str[1:len(str)])
	if e != nil {
		se := fmt.Sprintf("Corrupt File: invalid operation parameter count '%s' error:"+e.Error(), str[1:len(str)])
		err = UnexpectedEOF{msg: se}
		return
	}
	// read command
	command, e := readParameter(input)
	if e != nil {
		se := fmt.Sprintf("Corrupt File: Command can't be read. Error:" + e.Error())
		err = UnexpectedEOF{msg: se}
		return
	}

	if commandHasKey(command) {
		// read key
		key, e = readParameter(input)
		if e != nil {
			se := fmt.Sprintf("Corrupt File: key can't be read. Error:" + e.Error())
			err = UnexpectedEOF{msg: se}
			return
		}
		count-- // decrement count. as key counts as one
	}

	atts := make([]string, 0)
	for i := 1; i < count; i++ {
		// read attributes
		att, e := readParameter(input)
		if e != nil {
			se := fmt.Sprintf("Corrupt File: attribute pos:%d can't be read. Error:"+e.Error(), i)
			err = UnexpectedEOF{msg: se}
			return
		}
		atts = append(atts, att)
	}
	op.Command = command
	op.Key = key
	op.Arguments = atts
	return
}

func writeString(str string, out io.Writer) (err error) {
	size := len(str)
	s := fmt.Sprintf("$%d\r\n", size)
	n, err := out.Write([]byte(s))
	if err != nil {
		return
	}
	if n != len(s) {
		err = fmt.Errorf("Error writing length written %d expected %d", n, len(s))
		return
	}
	s = fmt.Sprintf("%s\r\n", str)
	n, err = out.Write([]byte(s))
	if err != nil {
		return
	}
	if n != len(s) {
		err = fmt.Errorf("Error writing length written %d expected %d", n, len(s))
		return
	}
	return
}

func (this Operation) ToAof(out io.Writer) (err error) {
	// write parameter count
	paramCount := 1 // 1 for command
	if commandHasKey(this.Command) {
		paramCount++ // count key
	}
	paramCount += len(this.Arguments)
	s := fmt.Sprintf("*%d\r\n", paramCount)
	n, err := out.Write([]byte(s))
	if err != nil {
		return
	}
	if n != len(s) {
		err = fmt.Errorf("Error writing length written %d expected %d", n, len(s))
		return
	}
	//write command
	err = writeString(this.Command, out)
	if err != nil {
		return
	}
	// write key
	if commandHasKey(this.Command) {
		err = writeString(this.Key, out)
		if err != nil {
			return
		}
	}
	// write attributes
	for i := 0; i < len(this.Arguments); i++ {
		err = writeString(this.Arguments[i], out)
		if err != nil {
			return
		}
	}
	return
}