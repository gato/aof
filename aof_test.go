package aoflib

import (
	"bufio"
	"io"
	"os"
	"testing"
)

func TestRead(t *testing.T) {
	file, err := os.Open("test-data.aof")
	if err != nil {
		t.Errorf("No se puede abrir archivo [%s].", err.Error())
		return
	}
	input := bufio.NewReader(file)
	op1, err := ReadOperation(input)
	if err != nil {
		t.Errorf("No se puede leer la operacion 1 [%s].", err.Error())
		return
	}
	if op1.Command != "SELECT" {
		t.Errorf("se esperaba el comando SELECT pero llego el comando [%s].", op1.Command)
		return
	}
	op2, err := ReadOperation(input)
	if err != nil {
		t.Errorf("No se puede leer la operacion 2 [%s].", err.Error())
		return
	}
	if op2.Command != "SET" {
		t.Errorf("se esperaba el comando SET pero llego el comando [%s].", op2.Command)
		return
	}
	op3, err := ReadOperation(input)
	if err != nil {
		t.Errorf("No se puede leer la operacion 3 [%s].", err.Error())
		return
	}
	if op3.Command != "SADD" {
		t.Errorf("se esperaba el comando SADD pero llego el comando [%s].", op3.Command)
		return
	}
	op4, err := ReadOperation(input)
	if err != nil {
		t.Errorf("No se puede leer la operacion 4 [%s].", err.Error())
		return
	}
	if op4.Command != "SADD" {
		t.Errorf("se esperaba el comando SADD pero llego el comando [%s].", op4.Command)
		return
	}
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("se esperaba que al intentar leer diera error")
		return
	}
	if err != io.EOF {
		t.Errorf("se esperaba que el error fuera [%s] sin embargo fue [%s]", io.EOF.Error(), err.Error())
		return
	}

}
func TestUnexpectedEofNoArguments(t *testing.T) {
	file, err := os.Open("test-data-eof1.aof")
	if err != nil {
		t.Errorf("No se puede abrir archivo [%s].", err.Error())
		return
	}
	input := bufio.NewReader(file)
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("se esperaba error leyendo la operacion 1 ")
		return
	}
	_, ok := err.(UnexpectedEOF)
	if !ok {
		t.Errorf("se esperaba que el error fuera UnexpectedEOF sin embargo fue [%s]", err.Error())
		return
	}
}
func TestUnexpectedEofInvalidNumberOfArguments(t *testing.T) {
	file, err := os.Open("test-data-eof2.aof")
	if err != nil {
		t.Errorf("No se puede abrir archivo [%s].", err.Error())
		return
	}
	input := bufio.NewReader(file)
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("se esperaba error leyendo la operacion 1 ")
		return
	}
	_, ok := err.(UnexpectedEOF)
	if !ok {
		t.Errorf("se esperaba que el error fuera UnexpectedEOF sin embargo fue [%s]", err.Error())
		return
	}
}
func TestUnexpectedEofInvalidCommandSize(t *testing.T) {
	file, err := os.Open("test-data-eof3.aof")
	if err != nil {
		t.Errorf("No se puede abrir archivo [%s].", err.Error())
		return
	}
	input := bufio.NewReader(file)
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("se esperaba error leyendo la operacion 1 ")
		return
	}
	_, ok := err.(UnexpectedEOF)
	if !ok {
		t.Errorf("se esperaba que el error fuera UnexpectedEOF sin embargo fue [%s]", err.Error())
		return
	}
}
func TestUnexpectedEofInvalidArgumentSize(t *testing.T) {
	file, err := os.Open("test-data-eof4.aof")
	if err != nil {
		t.Errorf("No se puede abrir archivo [%s].", err.Error())
		return
	}
	input := bufio.NewReader(file)
	_, err = ReadOperation(input)
	if err == nil {
		t.Errorf("se esperaba error leyendo la operacion 1 ")
		return
	}
	_, ok := err.(UnexpectedEOF)
	if !ok {
		t.Errorf("se esperaba que el error fuera UnexpectedEOF sin embargo fue [%s]", err.Error())
		return
	}
}

func TestSupportFlushall(t *testing.T) {
	file, err := os.Open("test-data-flushall.aof")
	if err != nil {
		t.Errorf("No se puede abrir archivo [%s].", err.Error())
		return
	}
	input := bufio.NewReader(file)
	op1, err := ReadOperation(input)
	if err != nil {
		t.Errorf("No se puede leer la operacion 1 [%s].", err.Error())
		return
	}
	if op1.Command != "FLUSHALL" {
		t.Errorf("se esperaba el comando FLUSHALL pero llego el comando [%s].", op1.Command)
		return
	}
	if len(op1.Arguments) != 0 {
		t.Errorf("flushall no tiene argumentos, pero se encontraron [%d].", len(op1.Arguments))
		return
	}
	_, err = ReadOperation(input)
	if err != nil {
		t.Errorf("No se puede leer la operacion 2 [%s].", err.Error())
		return
	}
}

func TestSupportFlushdb(t *testing.T) {
	file, err := os.Open("test-data-flushdb.aof")
	if err != nil {
		t.Errorf("No se puede abrir archivo [%s].", err.Error())
		return
	}
	input := bufio.NewReader(file)
	op1, err := ReadOperation(input)
	if err != nil {
		t.Errorf("No se puede leer la operacion 1 [%s].", err.Error())
		return
	}
	if op1.Command != "FLUSHDB" {
		t.Errorf("se esperaba el comando FLUSHDB pero llego el comando [%s].", op1.Command)
		return
	}
	if len(op1.Arguments) != 0 {
		t.Errorf("flushdb no tiene argumentos, pero se encontraron [%d].", len(op1.Arguments))
		return
	}
	_, err = ReadOperation(input)
	if err != nil {
		t.Errorf("No se puede leer la operacion 2 [%s].", err.Error())
		return
	}
}
