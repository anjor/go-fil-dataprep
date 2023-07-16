package split_and_commp

import (
	"encoding/csv"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/anjor/carlet"
	"github.com/urfave/cli/v2"
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
		Usage:    "Target size in bytes to chunk CARs to.",
	},
	&cli.StringFlag{
		Name:     "output",
		Aliases:  []string{"o"},
		Required: true,
		Usage:    "optional output filename prefix for car files.",
	},
	&cli.StringFlag{
		Name:     "metadata",
		Aliases:  []string{"m"},
		Required: false,
		Usage:    "optional metadata file name. Defaults to __metadata.csv",
		Value:    "__metadata.csv",
	},
}

func splitAndCommpAction(c *cli.Context) error {

	fi, err := getReader(c)
	if err != nil {
		return err
	}

	size := c.Int("size")
	output := c.String("output")
	meta := c.String("metadata")

	var filenamePrefix string

	if output != "" {
		filenamePrefix = fmt.Sprintf("%s-", output)
	}

	carFiles, err := carlet.SplitAndCommp(fi, size, filenamePrefix)
	if err != nil {
		return err
	}

	f, err := os.Create(meta)
	defer f.Close()
	if err != nil {
		return err
	}

	w := csv.NewWriter(f)
	err = w.Write([]string{"timestamp", "car file", "piece cid", "padded piece size"})
	if err != nil {
		return err
	}
	defer w.Flush()
	for _, c := range carFiles {
		err = w.Write([]string{
			time.Now().Format(time.RFC3339),
			c.Name,
			c.CommP.String(),
			strconv.FormatUint(c.PaddedSize, 10),
		})
	}
	return nil
}

func getReader(c *cli.Context) (io.Reader, error) {
	if c.Args().Present() {
		path := c.Args().First()
		fi, err := os.Open(path)
		if err != nil {
			return nil, err
		}
		return fi, nil

	}
	return os.Stdin, nil
}
