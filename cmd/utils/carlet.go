package utils

import (
	"bufio"
	"encoding/binary"
	"fmt"
	commcid "github.com/filecoin-project/go-fil-commcid"
	commp "github.com/filecoin-project/go-fil-commp-hashhash"
	"github.com/ipfs/go-cid"
	"io"
	"os"
)

const (
	BufSize          = (4 << 20) / 128 * 127
	varintSize       = 10
	nulRootCarHeader = "\x19" + // 25 bytes of CBOR (encoded as varint :cryingbear: )
		// map with 2 keys
		"\xA2" +
		// text-key with length 5
		"\x65" + "roots" +
		// 1 element array
		"\x81" +
		// tag 42
		"\xD8\x2A" +
		// bytes with length 5
		"\x45" +
		// nul-identity-cid prefixed with \x00 as required in DAG-CBOR: https://github.com/ipld/specs/blob/master/block-layer/codecs/dag-cbor.md#links
		"\x00\x01\x55\x00\x00" +
		// text-key with length 7
		"\x67" + "version" +
		// 1, we call this v0 due to the nul-identity CID being an open question: https://github.com/ipld/go-car/issues/26#issuecomment-604299576
		"\x01"
	maxBlockSize = 2 << 20 // 2 MiB
)

type CarFile struct {
	Name       string
	CarName    string
	CommP      cid.Cid
	PaddedSize uint64
}

func SplitAndCommp(r io.Reader, targetSize int, output string) ([]CarFile, error) {
	cp := new(commp.Calc)
	r = io.TeeReader(r, cp)
	var carFiles []CarFile

	streamBuf := bufio.NewReaderSize(r, BufSize)
	var streamLen int64

	streamLen, err := discardHeader(streamBuf, streamLen)
	if err != nil {
		return carFiles, err
	}

	var i int
	for {
		f := fmt.Sprintf("%s-%d.car", output, i)
		fmt.Printf("Writing file: %s\n", f)
		fi, err := os.Create(f)
		if err != nil {
			return carFiles, fmt.Errorf("failed to create file: %s\n", err)
		}
		if _, err := io.WriteString(fi, nulRootCarHeader); err != nil {
			return carFiles, fmt.Errorf("failed to write empty header: %s\n", err)
		}

		var carletLen int64
		for carletLen < int64(targetSize) {
			maybeNextFrameLen, err := streamBuf.Peek(varintSize)
			if err == io.EOF {
				rawCommP, paddedSize, err := cp.Digest()
				if err != nil {
					return carFiles, err
				}

				commCid, err := commcid.DataCommitmentV1ToCID(rawCommP)
				if err != nil {
					return carFiles, err
				}

				carFiles = append(carFiles,
					CarFile{
						Name:       output,
						CarName:    f,
						CommP:      commCid,
						PaddedSize: paddedSize,
					})
				return carFiles, nil
			}
			if err != nil && err != bufio.ErrBufferFull {
				return carFiles, fmt.Errorf("unexpected error at offset %d: %s\n", streamLen, err)
			}
			if len(maybeNextFrameLen) == 0 {
				return carFiles, fmt.Errorf("impossible 0-length peek without io.EOF at offset %d\n", streamLen)
			}

			frameLen, viL := binary.Uvarint(maybeNextFrameLen)
			if viL <= 0 {
				// car file with trailing garbage behind it
				return carFiles, fmt.Errorf("aborting car stream parse: undecodeable varint at offset %d", streamLen)
			}
			if frameLen > maxBlockSize {
				// anything over ~2MiB got to be a mistake
				return carFiles, fmt.Errorf("aborting car stream parse: unexpectedly large frame length of %d bytes at offset %d", frameLen, streamLen)
			}

			actualFrameLen, err := io.CopyN(fi, streamBuf, int64(viL)+int64(frameLen))
			streamLen += actualFrameLen
			carletLen += actualFrameLen
			if err != nil {
				if err != io.EOF {
					return carFiles, fmt.Errorf("unexpected error at offset %d: %s", streamLen-actualFrameLen, err)
				}
				rawCommP, paddedSize, err := cp.Digest()
				if err != nil {
					return carFiles, err
				}

				commCid, err := commcid.DataCommitmentV1ToCID(rawCommP)
				if err != nil {
					return carFiles, err
				}

				carFiles = append(carFiles,
					CarFile{
						Name:       output,
						CarName:    f,
						CommP:      commCid,
						PaddedSize: paddedSize,
					})
				return carFiles, nil
			}
		}

		rawCommP, paddedSize, err := cp.Digest()
		if err != nil {
			return carFiles, err
		}

		commCid, err := commcid.DataCommitmentV1ToCID(rawCommP)
		if err != nil {
			return carFiles, err
		}

		carFiles = append(carFiles,
			CarFile{
				Name:       output,
				CarName:    f,
				CommP:      commCid,
				PaddedSize: paddedSize,
			})

		fi.Close()
		i++
	}
}

func discardHeader(streamBuf *bufio.Reader, streamLen int64) (int64, error) {
	maybeHeaderLen, err := streamBuf.Peek(varintSize)
	if err != nil {
		return 0, fmt.Errorf("failed to read header: %s\n", err)
	}

	hdrLen, viLen := binary.Uvarint(maybeHeaderLen)
	if hdrLen <= 0 || viLen < 0 {
		return 0, fmt.Errorf("unexpected header len = %d, varint len = %d\n", hdrLen, viLen)
	}

	actualViLen, err := io.CopyN(io.Discard, streamBuf, int64(viLen))
	if err != nil {
		return 0, fmt.Errorf("failed to discard header varint: %s\n", err)
	}
	streamLen += actualViLen

	// ignoring header decoding for now
	actualHdrLen, err := io.CopyN(io.Discard, streamBuf, int64(hdrLen))
	if err != nil {
		return 0, fmt.Errorf("failed to discard header header: %s\n", err)
	}
	streamLen += actualHdrLen

	return streamLen, nil
}
