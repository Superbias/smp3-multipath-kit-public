package main

import (
	"crypto/sha256"
	"debug/buildinfo"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"os"
	"strings"
)

type binaryInfo struct {
	File         string `json:"file"`
	ExpectedOS   string `json:"expected_os"`
	ExpectedArch string `json:"expected_arch"`
	Format       string `json:"actual_format"`
	Arch         string `json:"actual_arch"`
	BuildGOOS    string `json:"build_goos,omitempty"`
	BuildGOARCH  string `json:"build_goarch,omitempty"`
	Module       string `json:"module,omitempty"`
	Size         int64  `json:"size"`
	SHA256       string `json:"sha256"`
	OK           bool   `json:"ok"`
	Error        string `json:"error,omitempty"`
}

type binaryHeader struct {
	format string
	arch   string
}

func main() {
	file := flag.String("file", "", "binary to inspect")
	target := flag.String("target", "", "expected target, for example linux/amd64")
	flag.Parse()
	if *file == "" || *target == "" {
		flag.Usage()
		os.Exit(2)
	}
	result, err := inspect(*file, *target)
	if err != nil {
		result = binaryInfo{File: *file, ExpectedOS: targetOS(*target), ExpectedArch: targetArch(*target), Error: err.Error()}
	}
	data, marshalErr := json.Marshal(result)
	if marshalErr != nil {
		fmt.Fprintln(os.Stderr, marshalErr)
		os.Exit(2)
	}
	fmt.Println(string(data))
	if !result.OK {
		os.Exit(1)
	}
}

func inspect(path, target string) (binaryInfo, error) {
	expectedOS, expectedArch, err := parseTarget(target)
	if err != nil {
		return binaryInfo{File: path, Error: err.Error()}, err
	}
	file, err := os.Open(path)
	if err != nil {
		return binaryInfo{File: path, ExpectedOS: expectedOS, ExpectedArch: expectedArch}, err
	}
	defer file.Close()
	stat, err := file.Stat()
	if err != nil {
		return binaryInfo{File: path, ExpectedOS: expectedOS, ExpectedArch: expectedArch}, err
	}
	digest := sha256.New()
	if _, err := io.Copy(digest, file); err != nil {
		return binaryInfo{File: path, ExpectedOS: expectedOS, ExpectedArch: expectedArch, Size: stat.Size()}, err
	}
	header, err := readHeader(file)
	if err != nil {
		return binaryInfo{File: path, ExpectedOS: expectedOS, ExpectedArch: expectedArch, Size: stat.Size(), SHA256: hex.EncodeToString(digest.Sum(nil))}, err
	}
	result := binaryInfo{
		File: path, ExpectedOS: expectedOS, ExpectedArch: expectedArch,
		Format: header.format, Arch: header.arch, Size: stat.Size(),
		SHA256: hex.EncodeToString(digest.Sum(nil)),
	}
	if info, infoErr := buildinfo.ReadFile(path); infoErr == nil {
		result.Module = info.Path
		for _, setting := range info.Settings {
			switch setting.Key {
			case "GOOS":
				result.BuildGOOS = setting.Value
			case "GOARCH":
				result.BuildGOARCH = setting.Value
			}
		}
	}
	result.OK = header.format == expectedFormat(expectedOS) && header.arch == expectedArch
	if result.OK && result.BuildGOOS != "" && result.BuildGOOS != expectedOS {
		result.OK = false
	}
	if result.OK && result.BuildGOARCH != "" && result.BuildGOARCH != expectedArch {
		result.OK = false
	}
	if !result.OK {
		result.Error = fmt.Sprintf("TARGET_MISMATCH: expected %s/%s, got format=%s arch=%s build_goos=%s build_goarch=%s", expectedOS, expectedArch, result.Format, result.Arch, result.BuildGOOS, result.BuildGOARCH)
	}
	return result, nil
}

func parseTarget(target string) (string, string, error) {
	parts := strings.Split(target, "/")
	if len(parts) != 2 || (parts[0] != "linux" && parts[0] != "windows") || parts[1] != "amd64" {
		return "", "", fmt.Errorf("unsupported target %q: expected linux/amd64 or windows/amd64", target)
	}
	return parts[0], parts[1], nil
}

func targetOS(target string) string {
	value, _, _ := parseTarget(target)
	return value
}

func targetArch(target string) string {
	_, value, _ := parseTarget(target)
	return value
}

func expectedFormat(expectedOS string) string {
	if expectedOS == "windows" {
		return "PE32+"
	}
	return "ELF64"
}

func readHeader(file *os.File) (binaryHeader, error) {
	var prefix [64]byte
	if _, err := file.ReadAt(prefix[:], 0); err != nil && !errors.Is(err, io.EOF) {
		return binaryHeader{}, err
	}
	if len(prefix) >= 20 && prefix[0] == 0x7f && prefix[1] == 'E' && prefix[2] == 'L' && prefix[3] == 'F' {
		if prefix[4] != 2 || prefix[5] != 1 {
			return binaryHeader{format: "ELF-non64-or-nonLE", arch: "unknown"}, nil
		}
		machine := binary.LittleEndian.Uint16(prefix[18:20])
		return binaryHeader{format: "ELF64", arch: elfArch(machine)}, nil
	}
	if prefix[0] == 'M' && prefix[1] == 'Z' {
		peOffset := int64(binary.LittleEndian.Uint32(prefix[0x3c:0x40]))
		var pe [26]byte
		if _, err := file.ReadAt(pe[:], peOffset); err != nil {
			return binaryHeader{format: "MS-DOS", arch: "unknown"}, err
		}
		if string(pe[:4]) != "PE\x00\x00" {
			return binaryHeader{format: "MS-DOS", arch: "unknown"}, nil
		}
		machine := binary.LittleEndian.Uint16(pe[4:6])
		optionalMagic := binary.LittleEndian.Uint16(pe[24:26])
		format := "PE32"
		if optionalMagic == 0x20b {
			format = "PE32+"
		}
		return binaryHeader{format: format, arch: peArch(machine)}, nil
	}
	if isMachO(binary.LittleEndian.Uint32(prefix[:4])) || isMachO(binary.BigEndian.Uint32(prefix[:4])) {
		return binaryHeader{format: "Mach-O", arch: "unknown"}, nil
	}
	return binaryHeader{format: "unknown", arch: "unknown"}, nil
}

func elfArch(machine uint16) string {
	if machine == 62 {
		return "amd64"
	}
	if machine == 183 {
		return "arm64"
	}
	return fmt.Sprintf("elf-machine-%d", machine)
}

func peArch(machine uint16) string {
	if machine == 0x8664 {
		return "amd64"
	}
	if machine == 0xaa64 {
		return "arm64"
	}
	return fmt.Sprintf("pe-machine-%#x", machine)
}

func isMachO(magic uint32) bool {
	switch magic {
	case 0xfeedface, 0xcefaedfe, 0xfeedfacf, 0xcffaedfe:
		return true
	default:
		return false
	}
}
