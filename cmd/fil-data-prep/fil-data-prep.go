package fil_data_prep

import (
	split_and_commp "data-prep/split-and-commp"
	"data-prep/utils"
	"encoding/json"
	"fmt"
	"github.com/anjor/anelace"
	"github.com/urfave/cli/v2"
	"io"
	"os"
	"strings"
	"sync"
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

	rerr, werr := io.Pipe()
	rout, wout := io.Pipe()

	anl, errs := anelace.NewAnelaceWithWriters(werr, wout)
	if errs != nil {
		return fmt.Errorf("unexpected error: %s\n", errs)
	}
	anl.SetMultipart(true)

	var fileReaders []io.Reader
	var files []string
	paths := c.Args().Slice()
	for _, path := range paths {
		pathInfo, err := os.Stat(path)
		if err != nil {
			return err
		}
		if !pathInfo.IsDir() {
			r, err := getFileReader(path, pathInfo)
			if err != nil {
				return err
			}
			files = append(files, path)
			fileReaders = append(fileReaders, r)
		} else {
			fs, frs, err := recursivelyGetFileReaders(path)
			if err != nil {
				return err
			}

			files = append(files, fs...)
			fileReaders = append(fileReaders, frs...)
		}
	}

	wg := sync.WaitGroup{}
	wg.Add(3)

	go func() {
		defer wg.Done()
		defer wout.Close()
		defer werr.Close()
		if err := anl.ProcessReader(io.MultiReader(fileReaders...), nil); err != nil {
			fmt.Printf("process reader error: %s", err)
		}
	}()

	o := c.String("output")
	m := c.String("metadata")
	s := c.Int("size")

	go func() {
		defer wg.Done()

		if err := split_and_commp.SplitAndCommp(rout, s, m, ".", o); err != nil {
			fmt.Printf("split and commp failed: %s", err)
		}
	}()

	var rs []roots
	go func() {
		defer wg.Done()

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

	}()

	wg.Wait()

	nodes := constructTree(files, rs)

	dirsCar, err := os.Create(fmt.Sprintf("%s-dirs.car", o))
	if err != nil {
		return err
	}
	_, err = io.WriteString(dirsCar, utils.NulRootCarHeader)
	if err != nil {
		return err
	}

	var cid, sizeVi []byte
	for _, nd := range nodes {
		cid = []byte(nd.Cid().String())
		d := nd.RawData()

		sizeVi = appendVarint(sizeVi[:0], uint64(len(cid))+uint64(len(d)))

		if _, err := dirsCar.Write(sizeVi); err == nil {
			fmt.Printf("sizeVi = %d\n", sizeVi)
			if _, err := dirsCar.Write(cid); err == nil {
				if _, err := dirsCar.Write(d); err != nil {
					return err
				}

			}
		}

		fmt.Printf("Node cid: %s\n", cid)
		for _, l := range nd.Links() {
			fmt.Printf("link = %s, %s\n", l.Name, l.Cid)
		}
	}

	dirsCar.Close()
	return nil
}
