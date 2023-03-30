package fil_data_prep

import (
	"encoding/csv"
	"encoding/json"
	"fmt"
	"github.com/anjor/anelace"
	"github.com/anjor/go-fil-dataprep/cmd/data-prep/utils"
	"github.com/ipfs/go-cid"
	"github.com/ipfs/go-merkledag"
	"github.com/urfave/cli/v2"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

var Cmd = &cli.Command{
	Name:    "fil-data-prep",
	Usage:   "end to end data prep",
	Aliases: []string{"dp"},
	Action:  filDataPrep,
	Flags: []cli.Flag{
		&cli.StringFlag{
			Name:     "output",
			Aliases:  []string{"o"},
			Required: false,
			Usage:    "optional output name for car files. Defaults to filename (stdin if streamed in from stdin).",
		},
		&cli.IntFlag{
			Name:     "size",
			Aliases:  []string{"s"},
			Required: false,
			Value:    2 << 20,
			Usage:    "Target size in bytes to chunk CARs to.",
		},
		&cli.StringFlag{
			Name:     "metadata",
			Aliases:  []string{"m"},
			Required: false,
			Value:    "__metadata.csv",
			Usage:    "metadata file name. ",
		},
	},
}

func filDataPrep(c *cli.Context) error {
	if !c.Args().Present() {
		return fmt.Errorf("expected some data to be processed, found none.\n")
	}

	var fileReaders []io.Reader
	var files []string
	paths := c.Args().Slice()

	for _, path := range paths {
		fs, frs, err := getAllFileReadersFromPath(path)
		if err != nil {
			return err
		}

		files = append(files, fs...)
		fileReaders = append(fileReaders, frs...)
	}

	wg := sync.WaitGroup{}
	wg.Add(3)

	rerr, werr := io.Pipe()
	rout, wout := io.Pipe()

	anl, errs := anelace.NewAnelaceWithWriters(werr, wout)
	if errs != nil {
		return fmt.Errorf("unexpected error: %s\n", errs)
	}
	anl.SetMultipart(true)

	go func() {
		defer wg.Done()
		defer werr.Close()
		if err := anl.ProcessReader(io.MultiReader(fileReaders...), nil); err != nil {
			fmt.Printf("process reader error: %s", err)
		}
	}()

	var rs []roots
	var rcid cid.Cid
	go func() {
		defer wg.Done()
		defer wout.Close()

		rs = getRoots(rerr)

		tr := constructTree(files, rs)
		nodes := getDirectoryNodes(tr)

		if len(paths) > 1 {
			rcid = nodes[0].Cid() // use fake root directory if multiple args
			writeNode(nodes, wout)
		} else {
			rcid = nodes[1].Cid() // otherwise use the first node (which should work)  todo: check this
			writeNode(nodes[1:], wout)
		}
	}()

	o := c.String("output")
	m := c.String("metadata")
	s := c.Int("size")

	go func() {
		defer wg.Done()

		carFiles, err := utils.SplitAndCommp(rout, s, o)
		if err != nil {
			fmt.Printf("split and commp failed : %s\n", err)
			return
		}

		f, err := os.Create(m)
		defer f.Close()
		if err != nil {
			fmt.Printf("creating metadata file failed: %s\n", m)
			return
		}
		w := csv.NewWriter(f)
		err = w.Write([]string{"timestamp", "original data", "car file", "root_cid", "piece cid", "padded piece size"})
		if err != nil {
			fmt.Printf("failed to write csv header\n")
			return
		}
		defer w.Flush()
		for _, c := range carFiles {
			err = w.Write([]string{
				time.Now().UTC().Format(time.RFC3339),
				o,
				rcid.String(),
				c.CarName,
				c.CommP.String(),
				strconv.FormatUint(c.PaddedSize, 10),
			})
		}
	}()

	wg.Wait()

	fmt.Printf("root cid = %s\n", rcid)

	return nil
}

func writeNode(nodes []*merkledag.ProtoNode, wout *io.PipeWriter) {
	var cid, sizeVi []byte
	for _, nd := range nodes {
		cid = []byte(nd.Cid().KeyString())
		d := nd.RawData()

		sizeVi = appendVarint(sizeVi[:0], uint64(len(cid))+uint64(len(d)))

		if _, err := wout.Write(sizeVi); err == nil {
			if _, err := wout.Write(cid); err == nil {
				if _, err := wout.Write(d); err != nil {
					fmt.Printf("failed to write car: %s\n", err)
				}

			}
		}

	}
}

func getRoots(rerr *io.PipeReader) []roots {

	var rs []roots
	bs, _ := io.ReadAll(rerr)
	e := string(bs)
	els := strings.Split(e, "\n")
	for _, el := range els {
		if el == "" {
			continue
		}
		var r roots
		err := json.Unmarshal([]byte(el), &r)
		if err != nil {
			fmt.Printf("failed to unmarshal json: %s\n", el)
		}
		rs = append(rs, r)
	}
	return rs
}
