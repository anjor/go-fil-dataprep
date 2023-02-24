# go-carbites-commp

Chunking for [CAR files](https://ipld.io/specs/transport/car/). Split a single CAR into multiple CARs, but also calculates [commP](https://spec.filecoin.io/#section-systems.filecoin_files.piece) for the CAR files.

Inspired by [go-carbites](https://github.com/alanshaw/go-carbites).

## Usage

The CLI takes a car file, either as an argument

```
~/repos/anjor/go-carbites-commp/cmd/go-carbites-commp split-and-commp --size 1000000000 file.car
```

or from stdin
```
cat 5gb-filecoin-payload.bin | ~/repos/ribasushi/DAGger/bin/stream-dagger --ipfs-add-compatible-command="--cid-version=1" --emit-stdout=car-v0-pinless-stream  | ~/repos/anjor/go-carbites-commp/cmd/go-carbites-commp split-and-commp --size 1000000000
```
