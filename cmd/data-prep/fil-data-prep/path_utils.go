package fil_data_prep

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
)

func getFileReader(path string, pathInfo os.FileInfo) (io.Reader, error) {
	if pathInfo.IsDir() {
		return nil, fmt.Errorf("expect file got directory: %s", path)
	}
	fileSize := pathInfo.Size()

	sizeBytes := make([]byte, 8)
	binary.BigEndian.PutUint64(sizeBytes, uint64(fileSize))

	fi, err := os.Open(path)
	if err != nil {
		return nil, err
	}
	return io.MultiReader(bytes.NewReader(sizeBytes), fi), nil
}

func recursivelyGetFileReaders(path string) (files []string, frs []io.Reader, err error) {
	err = filepath.WalkDir(path, func(p string, d fs.DirEntry, e error) error {
		if e != nil {
			return e
		}

		if d.IsDir() {
			return nil
		}

		files = append(files, p)
		di, err := d.Info()
		if err != nil {
			return err
		}
		r, err := getFileReader(p, di)
		if err != nil {
			return err
		}
		frs = append(frs, r)
		return nil
	})

	if err != nil {
		return nil, nil, err
	}

	return
}

func getAllFileReadersFromPath(path string) ([]string, []io.Reader, error) {

	pathInfo, err := os.Stat(path)
	if err != nil {
		return nil, nil, err
	}

	if !pathInfo.IsDir() {

		r, err := getFileReader(path, pathInfo)
		if err != nil {
			return nil, nil, err
		}
		return []string{path}, []io.Reader{r}, nil
	}

	return recursivelyGetFileReaders(path)
}
