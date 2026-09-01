package main

import (
	"encoding/binary"
	"os"
	"path/filepath"
	"testing"
)

func TestTargetParserAcceptsOnlySupportedTargets(t *testing.T) {
	for _, target := range []string{"linux/amd64", "windows/amd64"} {
		if _, _, err := parseTarget(target); err != nil {
			t.Fatal(err)
		}
	}
	for _, target := range []string{"linux/arm64", "windows/386", "darwin/amd64"} {
		if _, _, err := parseTarget(target); err == nil {
			t.Fatalf("unsupported target accepted: %s", target)
		}
	}
}

func TestContentBasedFormatAndArchitectureChecks(t *testing.T) {
	tests := []struct {
		name   string
		data   []byte
		target string
		want   bool
	}{
		{name: "elf-amd64-linux", data: fakeELF(62), target: "linux/amd64", want: true},
		{name: "pe-amd64-windows", data: fakePE(0x8664, 0x20b), target: "windows/amd64", want: true},
		{name: "pe-named-linux", data: fakePE(0x8664, 0x20b), target: "linux/amd64", want: false},
		{name: "elf-named-windows", data: fakeELF(62), target: "windows/amd64", want: false},
		{name: "elf-arm64-as-amd64", data: fakeELF(183), target: "linux/amd64", want: false},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), test.name)
			if err := os.WriteFile(path, test.data, 0o600); err != nil {
				t.Fatal(err)
			}
			result, err := inspect(path, test.target)
			if err != nil {
				t.Fatal(err)
			}
			if result.OK != test.want {
				t.Fatalf("ok=%t want=%t result=%#v", result.OK, test.want, result)
			}
		})
	}
}

func TestFilenameDoesNotAffectValidation(t *testing.T) {
	data := fakePE(0x8664, 0x20b)
	root := t.TempDir()
	first := filepath.Join(root, "linux-amd64")
	second := filepath.Join(root, "anything.bin")
	if err := os.WriteFile(first, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(second, data, 0o600); err != nil {
		t.Fatal(err)
	}
	left, err := inspect(first, "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	right, err := inspect(second, "linux/amd64")
	if err != nil {
		t.Fatal(err)
	}
	if left.OK || right.OK || left.Error != right.Error {
		t.Fatalf("filename influenced result: left=%#v right=%#v", left, right)
	}
}

func fakeELF(machine uint16) []byte {
	data := make([]byte, 128)
	copy(data[:4], []byte{0x7f, 'E', 'L', 'F'})
	data[4] = 2
	data[5] = 1
	binary.LittleEndian.PutUint16(data[18:20], machine)
	return data
}

func fakePE(machine, optionalMagic uint16) []byte {
	data := make([]byte, 160)
	data[0], data[1] = 'M', 'Z'
	binary.LittleEndian.PutUint32(data[0x3c:0x40], 0x40)
	copy(data[0x40:0x44], []byte{'P', 'E', 0, 0})
	binary.LittleEndian.PutUint16(data[0x44:0x46], machine)
	binary.LittleEndian.PutUint16(data[0x58:0x5a], optionalMagic)
	return data
}
