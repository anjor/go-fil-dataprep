package fil_data_prep

import (
	"bytes"
	"encoding/binary"
	"fmt"
	"io"
	"os"
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
