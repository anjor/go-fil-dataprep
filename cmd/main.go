package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/alanshaw/go-carbites"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"io"
	"log"
	"os"
	"path/filepath"
	"strconv"
	"strings"

	"github.com/urfave/cli/v2"
)

var splitAndCommPCmd = &cli.Command{
	Name: "split-and-commp",
	Action: func(c *cli.Context) error {

		dir, name, fi, err := getReader(c)
		if err != nil {
			return err
		}

		cp := new(commp.Calc)
		fi = io.TeeReader(fi, cp)
		size := c.Int("size")
		splitter, err := carbites.NewSimpleSplitter(fi, size)

		f, err := os.Create("__metadata.csv")
		defer f.Close()
		if err != nil {
			return err
		}
		w := csv.NewWriter(f)
		w.Write([]string{"name", "car", "commp", "padded_piece_size", "unpadded_piece_size"})
		defer w.Flush()
		var i int
		for {
			r, err := splitter.Next()
			if err != nil {
				if err == io.EOF {
					break
				}
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

			carChunkPath := fmt.Sprintf("%s/%s-%d.car", dir, name, i)
			fmt.Printf("Writing CAR chunk to %s\n", carChunkPath)
			fi, err := os.Create(carChunkPath)
			if err != nil {
				return err
			}

			br := bufio.NewReader(r)
			_, err = br.WriteTo(fi)
			if err != nil {
				return err
			}
			fi.Close()
			err = w.Write([]string{
				name,                               // original file
				fmt.Sprintf("%s-%d.car", name, i),  // car file
				commCid.String(),                   // commp
				strconv.FormatUint(paddedSize, 10), // padded size
				strconv.FormatUint(paddedSize/128*127, 10)}, //unpadded size
			)
			if err != nil {
				return err
			}
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

func getReader(c *cli.Context) (string, string, io.Reader, error) {
	if c.Args().Present() {
		path := c.Args().First()
		dir := filepath.Dir(path)
		name := strings.TrimSuffix(filepath.Base(path), ".car")

		fi, err := os.Open(path)
		if err != nil {
			return "", "", nil, err
		}

		return dir, name, fi, nil

	}
	return ".", "stdin", os.Stdin, nil
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
