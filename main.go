package main

import (
	"bufio"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"

	"github.com/alanshaw/go-carbites"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"

	"github.com/urfave/cli/v2"
)

var splitAndCommPCmd = &cli.Command{
	Name: "split-and-commp",
	Action: func(c *cli.Context) error {
		size := c.Int("size")

		var dir, name string
		var fi io.Reader
		var err error

		cp := new(commp.Calc)

		if c.Args().Present() {
			path := c.Args().First()
			dir = filepath.Dir(path)
			name = strings.TrimSuffix(filepath.Base(path), ".car")

			fi, err = os.Open(path)
			fi = io.TeeReader(fi, cp)
			if err != nil {
				return err
			}
		} else {
			dir = "."
			name = "stdin"

			fi = io.TeeReader(os.Stdin, cp)
		}

		splitter, err := carbites.NewSimpleSplitter(fi, size)

		var i int
		for {
			r, err := splitter.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
				return err
			}
			writePath := fmt.Sprintf("%s/%s-%d.car", dir, name, i)
			fmt.Printf("Writing CAR chunk to %s\n", writePath)
			fi, err := os.Create(writePath)
			if err != nil {
				return err
			}

			rawCommP, paddedSize, err := cp.Digest()
			if err != nil {
				log.Fatal(err)
			}

			commCid, err := commcid.DataCommitmentV1ToCID(rawCommP)
			if err != nil {
				log.Fatal(err)
			}

			br := bufio.NewReader(r)
			_, err = br.WriteTo(fi)
			if err != nil {
				return err
			}
			fi.Close()

			fmt.Fprintf(os.Stderr, `
CommPCid: %s
Unpadded piece: % 12d bytes
Padded piece:   % 12d bytes
`,
				commCid,
				paddedSize/128*127,
				paddedSize,
			)
			i++
		}

		return nil
	},
	Flags: []cli.Flag{
		&cli.IntFlag{
			Name:     "size",
			Required: true,
			Value:    2 << 20,
			Usage:    "Target size in bytes to chunk CARs to.",
		},
	},
}

func main() {
	app := cli.NewApp()
	app.Name = "carbites-commp"
	app.Usage = "Chunking for CAR files + calculating commP. Splits a CAR file into smaller CAR files and at the same time also calculates commP for the smaller CAR files."
	app.Commands = []*cli.Command{
		splitAndCommPCmd,
	}
	err := app.Run(os.Args)
	if err != nil {
		fmt.Printf("Error: %v\n", err)
		return
	}
}
