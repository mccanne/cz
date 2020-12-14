package main

import (
	"encoding/csv"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
)

// takes stdin as csv and outputs as json

func usage() {
	fmt.Fprintln(os.Stderr, "usage: cj [-s]")
	os.Exit(1)
}

func main() {
	if len(os.Args) > 2 {
		usage()
	}
	var stringsOnly bool
	if len(os.Args) == 2 {
		if os.Args[1] == "-s" {
			stringsOnly = true
		} else {
			usage()
		}
	}
	r := csv.NewReader(os.Stdin)
	var hdr []string
	object := make(map[string]interface{})
	var line int
	for {
		rec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				return
			}
			log.Fatal(err)
		}
		line++
		if hdr == nil {
			hdr = rec
			continue
		}
		if err := translate(hdr, rec, object, stringsOnly); err != nil {
			log.Fatal(fmt.Errorf("line %d: %s", line, err))
		}
	}
}

func translate(hdr, rec []string, object map[string]interface{}, stringsOnly bool) error {
	if len(hdr) != len(rec) {
		return errors.New("length of record doesn't match heading")
	}
	for k, field := range hdr {
		val := rec[k]
		if stringsOnly {
			object[field] = val
		}
		lower := strings.ToLower(val)
		if lower == "+inf" || lower == "inf" {
			object[field] = math.MaxFloat64
		} else if lower == "-inf" {
			object[field] = -math.MaxFloat64
		} else if lower == "nan" {
			object[field] = "NaN"
		} else if strings.TrimSpace(val) == "" {
			object[field] = nil
		} else if v, err := strconv.ParseFloat(val, 64); err == nil {
			object[field] = v
		} else if v, err := strconv.ParseBool(val); err == nil {
			object[field] = v
		} else {
			object[field] = val
		}
	}
	b, err := json.Marshal(object)
	if err != nil {
		return err
	}
	fmt.Println(string(b))
	return nil
}
