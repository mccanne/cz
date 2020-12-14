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

	"github.com/brimsec/zq/field"
	"github.com/brimsec/zq/zio/zngio"
	"github.com/brimsec/zq/zng"
	"github.com/brimsec/zq/zng/builder"
	"github.com/brimsec/zq/zng/resolver"
	"github.com/brimsec/zq/zng/typevector"
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
	zctx := resolver.NewContext()
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
			c = newConverter(zctx, csvRec, stringsOnly)
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
	zctx        *resolver.Context
	types       *typevector.Table
	hdr         []string
	stringsOnly bool
	builder     *builder.ColumnBuilder
	cache       []zng.Value
	recTypes    map[int]*zng.TypeRecord
}

func newConverter(zctx *resolver.Context, hdr []string, stringsOnly bool) *converter {
	var fields []field.Static
	for _, name := range hdr {
		fields = append(fields, field.New(name))
	}
	b, _ := builder.NewColumnBuilder(zctx, fields)
	return &converter{
		zctx:        zctx,
		hdr:         hdr,
		builder:     b,
		types:       typevector.NewTable(),
		cache:       make([]zng.Value, len(hdr)),
		recTypes:    make(map[int]*zng.TypeRecord),
		stringsOnly: stringsOnly,
	}
}

func (c *converter) translate(fields []string) (*zng.Record, error) {
	if len(fields) != len(c.cache) {
		return nil, errors.New("length of record doesn't match heading")
	}
	vals := c.cache[:0]
	for _, field := range fields {
		//if stringsOnly {
		//	object[field] = val
		//}
		var zv zng.Value
		lower := strings.ToLower(field)
		if lower == "+inf" || lower == "inf" {
			zv = zng.NewFloat64(math.MaxFloat64)
		} else if lower == "-inf" {
			zv = zng.NewFloat64(-math.MaxFloat64)
		} else if lower == "nan" {
			zv = zng.NewFloat64(math.NaN())
		} else if strings.TrimSpace(field) == "" {
			zv = zng.Value{zng.TypeNull, nil}
		} else if v, err := strconv.ParseFloat(field, 64); err == nil {
			zv = zng.NewFloat64(v)
		} else if v, err := strconv.ParseBool(field); err == nil {
			zv = zng.NewBool(v)
		} else {
			zv = zng.NewString(field)
		}
		vals = append(vals, zv)
	}
	if len(vals) != len(c.hdr) {
		return nil, errors.New("values columns don't match header columns")
	}
	id := c.types.LookupByValues(vals)
	typ, ok := c.recTypes[id]
	if !ok {
		types := make([]zng.Type, 0, len(vals))
		for _, v := range vals {
			types = append(types, v.Type)
		}
		cols := c.builder.TypedColumns(types)
		var err error
		typ, err = c.zctx.LookupTypeRecord(cols)
		if err != nil {
			return nil, err
		}
		c.recTypes[id] = typ
	}
	b := c.builder
	b.Reset()
	for _, zv := range vals {
		b.Append(zv.Bytes, false)
	}
	bytes, _ := b.Encode()
	return zng.NewRecord(typ, bytes), nil
}
