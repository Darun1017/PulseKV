package main

import (
	"bufio"
	"fmt"
	"io"
	"net/http"
	"os"
	"strings"
)

const defaultAddr = "http://localhost:8080"

func main() {
	addr := defaultAddr

	// Parse optional --addr flag anywhere in args.
	args := os.Args[1:]
	var filtered []string
	for i := 0; i < len(args); i++ {
		if args[i] == "--addr" && i+1 < len(args) {
			addr = args[i+1]
			i++ // skip value
		} else if strings.HasPrefix(args[i], "--addr=") {
			addr = strings.TrimPrefix(args[i], "--addr=")
		} else {
			filtered = append(filtered, args[i])
		}
	}
	args = filtered

	if len(args) == 0 {
		usage()
		os.Exit(1)
	}

	switch args[0] {
	case "put":
		if len(args) < 3 {
			fmt.Fprintln(os.Stderr, "Usage: pulsectl put <key> <value>")
			os.Exit(1)
		}
		doPut(addr, args[1], strings.Join(args[2:], " "))
	case "get":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: pulsectl get <key>")
			os.Exit(1)
		}
		doGet(addr, args[1])
	case "delete":
		if len(args) < 2 {
			fmt.Fprintln(os.Stderr, "Usage: pulsectl delete <key>")
			os.Exit(1)
		}
		doDelete(addr, args[1])
	case "watch":
		doWatch(addr)
	default:
		fmt.Fprintf(os.Stderr, "Unknown command: %s\n", args[0])
		usage()
		os.Exit(1)
	}
}

func usage() {
	fmt.Fprintln(os.Stderr, `PulseKV CLI — pulsectl

Usage:
  pulsectl [--addr=URL] <command> [args...]

Commands:
  put <key> <value>   Store a value
  get <key>           Retrieve a value
  delete <key>        Delete a key
  watch               Stream live events (SSE)

Flags:
  --addr URL          Cluster address (default: http://localhost:8080)`)
}

// ---- Commands ---------------------------------------------------------------

func doPut(addr, key, value string) {
	url := fmt.Sprintf("%s/v1/%s", addr, key)

	req, err := http.NewRequest(http.MethodPut, url, strings.NewReader(value))
	if err != nil {
		fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "ERROR %d: %s\n", resp.StatusCode, body)
		os.Exit(1)
	}
	fmt.Println(string(body))
}

func doGet(addr, key string) {
	url := fmt.Sprintf("%s/v1/%s", addr, key)

	resp, err := http.Get(url)
	if err != nil {
		fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "ERROR %d: %s\n", resp.StatusCode, body)
		os.Exit(1)
	}
	fmt.Println(string(body))
}

func doDelete(addr, key string) {
	url := fmt.Sprintf("%s/v1/%s", addr, key)

	req, err := http.NewRequest(http.MethodDelete, url, nil)
	if err != nil {
		fatal(err)
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		fatal(err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if resp.StatusCode != http.StatusOK {
		fmt.Fprintf(os.Stderr, "ERROR %d: %s\n", resp.StatusCode, body)
		os.Exit(1)
	}
	fmt.Println(string(body))
}

func doWatch(addr string) {
	url := fmt.Sprintf("%s/v1/watch", addr)

	resp, err := http.Get(url)
	if err != nil {
		fatal(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		body, _ := io.ReadAll(resp.Body)
		fmt.Fprintf(os.Stderr, "ERROR %d: %s\n", resp.StatusCode, body)
		os.Exit(1)
	}

	fmt.Println("Watching for events (Ctrl+C to stop)…")
	scanner := bufio.NewScanner(resp.Body)
	for scanner.Scan() {
		line := scanner.Text()
		if strings.HasPrefix(line, "data: ") {
			fmt.Println(strings.TrimPrefix(line, "data: "))
		}
	}
	if err := scanner.Err(); err != nil {
		fmt.Fprintf(os.Stderr, "watch stream error: %v\n", err)
	}
}

func fatal(err error) {
	fmt.Fprintf(os.Stderr, "Error: %v\n", err)
	os.Exit(1)
}
