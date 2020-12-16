package main

import (
	"encoding/csv"
	"errors"
	"fmt"
	"io"
	"log"
	"math"
	"os"
	"strconv"
	"strings"

	"github.com/brimsec/zq/zio/zngio"
	"github.com/brimsec/zq/zng"
	"github.com/mccanne/z"
)

// takes stdin as csv and send binary zng to stdout

func usage() {
	fmt.Fprintln(os.Stderr, "usage: cz [-s]")
	os.Exit(1)
}

type nopCloser struct {
	io.Writer
}

func (*nopCloser) Close() error {
	return nil
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
	w := zngio.NewWriter(&nopCloser{os.Stdout}, zngio.WriterOpts{LZ4BlockSize: zngio.DefaultLZ4BlockSize})
	r := csv.NewReader(os.Stdin)
	var c *converter
	var line int
	for {
		csvRec, err := r.Read()
		if err != nil {
			if err == io.EOF {
				w.Close()
				return
			}
			log.Fatal(err)
		}
		line++
		if c == nil {
			c = newConverter(csvRec, stringsOnly)
			continue
		}
		rec, err := c.translate(csvRec)
		if err != nil {
			log.Fatal(fmt.Errorf("line %d: %s", line, err))
		}
		if err := w.Write(rec); err != nil {
			log.Fatal(fmt.Errorf("line %d: %s", line, err))
		}
	}
}

type converter struct {
	builder *z.Builder
	strings bool
	hdr     []string
	vals    []interface{}
}

func newConverter(hdr []string, stringsOnly bool) *converter {
	return &converter{
		builder: z.NewBuilder(),
		hdr:     hdr,
		vals:    make([]interface{}, len(hdr)),
		strings: stringsOnly,
	}
}

func (c *converter) translate(fields []string) (*zng.Record, error) {
	if len(fields) != len(c.vals) {
		return nil, errors.New("length of record doesn't match heading")
	}
	vals := c.vals[:0]
	for _, field := range fields {
		if c.strings {
			vals = append(vals, field)
		} else {
			vals = append(vals, convertString(field))
		}
	}
	return c.builder.FromFields(c.hdr, vals)
}

func convertString(s string) interface{} {
	lower := strings.ToLower(s)
	switch lower {
	case "+inf", "inf":
		return math.MaxFloat64
	case "-inf":
		return -math.MaxFloat64
	case "nan":
		return math.NaN()
	case "":
		// XXX library should handle coding nil value slice...
		// for now it crashes and we insert this so we get
		// a null(null) instead of an unset column
		var v interface{}
		return &v
	}
	if v, err := strconv.ParseFloat(s, 64); err == nil {
		return v
	}
	if v, err := strconv.ParseBool(s); err == nil {
		return v
	}
	return s
}
