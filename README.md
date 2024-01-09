# global-nosql-benchmark
Identify and address performance disparities, optimize scalability across regions, and ensure your databases deliver consistent, reliable performance worldwide. Don't settle for single-zone limitations – embrace a comprehensive approach for peak performance and reliability.




## Installation

### Download Standalone binaries ( no Golang needed )

If you don't have go on your machine and just want to use the produced binaries you can download the following prebuilt bins:

https://github.com/filipecosta90/global-nosql-benchmark/releases/latest

| OS | Arch | Link |
| :---         |     :---:      |          ---: |
| Linux   | amd64  (64-bit X86)     | [global-nosql-benchmark-linux-amd64](https://github.com/filipecosta90/global-nosql-benchmark/releases/latest/download/global-nosql-benchmark-linux-amd64.tar.gz)    |
| Linux   | arm64 (64-bit ARM)     | [global-nosql-benchmark-linux-arm64](https://github.com/filipecosta90/global-nosql-benchmark/releases/latest/download/global-nosql-benchmark-linux-arm64.tar.gz)    |
| Darwin   | amd64  (64-bit X86)     | [global-nosql-benchmark-darwin-amd64](https://github.com/filipecosta90/global-nosql-benchmark/releases/latest/download/global-nosql-benchmark-darwin-amd64.tar.gz)    |
| Darwin   | arm64 (64-bit ARM)     | [global-nosql-benchmark-darwin-arm64](https://github.com/filipecosta90/global-nosql-benchmark/releases/latest/download/global-nosql-benchmark-darwin-arm64.tar.gz)    |

Here's how bash script to download and try it:

```bash
wget -c https://github.com/filipecosta90/global-nosql-benchmark/releases/latest/download/global-nosql-benchmark-$(uname -mrs | awk '{ print tolower($1) }')-$(dpkg --print-architecture).tar.gz -O - | tar -xz

# give it a try
./global-nosql-benchmark --help
```


### Installation in a Golang env

To install the benchmark utility with a Go Env do as follow:

`go get` and then `go install`:
```bash
# Fetch this repo
go get github.com/filipecosta90/global-nosql-benchmark
cd $GOPATH/src/github.com/filipecosta90/global-nosql-benchmark
make
```