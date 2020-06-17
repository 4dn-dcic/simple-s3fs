# simple-s3fs

A simple (read-only) FUSE-based file system for S3 buckets.

## Usage

```
simple-s3fs --aws-profile default -f -l /tmp/s3.log /tmp/s3/<bucket>
less /tmp/s3/<bucket>/folder/file.txt
```

Please note: Folders in buckets cannot have `.` in their names and files must have an extension (i.e. a `.`)


## Unmounting

```
umount /tmp/s3/<bucket>
```

## Development

The recommended way to develop `simple-s3fs` is to use a [conda](https://conda.io/docs/intro.html) environment and
install `simple-s3fs` with develop mode:

```shell
pip install -e .
```


### Important

 This is a FORK of Peter Kerpedjiev's [simple-httpfs](https://github.com/higlass/simple-httpfs)
 package. It was adapted to the needs of projects at the
 [4D Nucleome Data Coordination and Integration Center (4DN-DCIC)](https://github.com/4dn-dcic).
