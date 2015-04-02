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
	Key       string // por ahora los leo todos como parametros hasta que sepa que comandos si y cuales no tiene key. (ejemplo select)
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

func leerParametro(input *bufio.Reader) (s string, err error) {
	// leo la longitud del parametro
	str, err := readLine(input)
	if err != nil {
		return
	}
	if len(str) < 2 {
		err = UnexpectedEOF{msg: "Leyendo la longitud del parametro"}
		return
	}
	if string(str[0]) != "$" {
		se := fmt.Sprintf("Archivo corrupto: El elemento no es la longitud del parametro")
		err = UnexpectedEOF{msg: se}
		return
	}
	size, e := strconv.Atoi(str[1:len(str)])
	if e != nil {
		se := fmt.Sprintf("Archivo corrupto: La cantidad de elementos no es correcta '%s' error:"+e.Error(), str[1:len(str)])
		err = UnexpectedEOF{msg: se}
		return
	}
	// leo el parametro
	str, err = readLine(input)
	if err != nil {
		return
	}
	if len(str) != size {
		se := fmt.Sprintf("Leyendo parametro se esperaban %d caracteres y se leyeron %d >'%s'<", size, len(str), str)
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
	// leo la cantidad de parametros
	var key string
	str, err := readLine(input)
	if err != nil {
		return
	}
	if len(str) < 2 {
		err = UnexpectedEOF{msg: "Leyendo la longitud del parametro"}
		return
	}
	if string(str[0]) != "*" {
		se := fmt.Sprintf("Archivo corrupto: El primer elemento no es la cantidad de parametros")
		err = UnexpectedEOF{msg: se}
		return
	}
	count, e := strconv.Atoi(str[1:len(str)])
	if e != nil {
		se := fmt.Sprintf("Archivo corrupto: La cantidad de elementos no es correcta '%s' error:"+e.Error(), str[1:len(str)])
		err = UnexpectedEOF{msg: se}
		return
	}
	// leer commando
	command, e := leerParametro(input)
	if e != nil {
		se := fmt.Sprintf("Archivo corrupto: No se puede leer el comando error:" + e.Error())
		err = UnexpectedEOF{msg: se}
		return
	}

	if commandHasKey(command) { // por ahora el unico que se que no opera sobre keys es select y FLUSHDB no opera con nada
		// leer key
		key, e = leerParametro(input)
		if e != nil {
			se := fmt.Sprintf("Archivo corrupto: No se puede leer el key error:" + e.Error())
			err = UnexpectedEOF{msg: se}
			return
		}
		count-- // descuento la cantidad de parametros ya que el key esta incluido en los mismos
	}

	atts := make([]string, 0)
	for i := 1; i < count; i++ {
		// leer los atributos
		att, e := leerParametro(input)
		if e != nil {
			se := fmt.Sprintf("Archivo corrupto: No se puede leer el attributo %d error:"+e.Error(), i)
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
		err = fmt.Errorf("la cantidad escrita no es igual a la enviada")
		return
	}
	s = fmt.Sprintf("%s\r\n", str)
	n, err = out.Write([]byte(s))
	if err != nil {
		return
	}
	if n != len(s) {
		err = fmt.Errorf("la cantidad escrita no es igual a la enviada")
		return
	}
	return
}

func (this Operation) ToAof(out io.Writer) (err error) {
	// escribir cantidad de parametros
	paramCount := 1                  // commando
	if commandHasKey(this.Command) { // por ahora el unico que se que no opera sobre keys es select
		paramCount++ // el key
	}
	paramCount += len(this.Arguments)
	s := fmt.Sprintf("*%d\r\n", paramCount)
	n, err := out.Write([]byte(s))
	if err != nil {
		return
	}
	if n != len(s) {
		err = fmt.Errorf("la cantidad escrita no es igual a la enviada")
		return
	}
	//escribir commando
	err = writeString(this.Command, out)
	if err != nil {
		return
	}
	// escribir key
	if commandHasKey(this.Command) {
		err = writeString(this.Key, out)
		if err != nil {
			return
		}
	}
	// escribir atributos
	for i := 0; i < len(this.Arguments); i++ {
		// leer los atributos
		err = writeString(this.Arguments[i], out)
		if err != nil {
			return
		}
	}
	return
}
