package split_and_commp

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"github.com/alanshaw/go-carbites"
	"github.com/anjor/go-fil-dataprep/cmd/data-prep/utils"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/urfave/cli/v2"
	"io"
	"log"
	"os"
	"strconv"
)

var Cmd = &cli.Command{

	Name:    "split-and-commp",
	Usage:   "Split CAR and calculate commp",
	Aliases: []string{"sac"},
	Action:  splitAndCommpAction,
	Flags:   splitAndCommpFlags,
}

var splitAndCommpFlags = []cli.Flag{
	&cli.IntFlag{
		Name:     "size",
		Aliases:  []string{"s"},
		Required: true,

		Usage: "Target size in bytes to chunk CARs to.",
	},
	&cli.StringFlag{
		Name:     "output",
		Aliases:  []string{"o"},
		Required: false,
		Usage:    "optional output name for car files. Defaults to filename (stdin if streamed in from stdin).",
	},
	&cli.StringFlag{
		Name:     "metadata",
		Aliases:  []string{"m"},
		Required: false,
		Usage:    "optional metadata file name. Defaults to __metadata.csv",
	},
}

func splitAndCommpAction(c *cli.Context) error {

	dir, name, fi, err := utils.GetReader(c)
	if err != nil {
		return err
	}

	size := c.Int("size")
	output := c.String("output")
	if output != "" {
		name = output
	}

	meta := c.String("metadata")
	if meta == "" {
		meta = "__metadata.csv"
	}

	return SplitAndCommp(fi, size, meta, dir, name)
}

func SplitAndCommp(r io.Reader, size int, metadata, dir, name string) error {
	cp := new(commp.Calc)
	r = io.TeeReader(r, cp)
	splitter, err := carbites.NewSimpleSplitter(r, size)

	f, err := os.Create(metadata)
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

}
