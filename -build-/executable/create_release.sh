#!/bin/sh

echo "Pruning the old releases..."
rm -R releases

echo "Creating folders..."
mkdir releases
mkdir releases/linux
mkdir releases/macosx
mkdir releases/windows

major=$(date +%y)
buildNo=`printf %04d $(expr $(expr $(date +%s) - $(gdate -d "Jun 13 2020" +%s)) / 345600)`
export RELEASE_VERSION="$major.2.$buildNo"

echo ""
echo "Building FileSystem command-line tool (v$RELEASE_VERSION)"
cd ../../fs-tool
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/kertish-dfs
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/kertish-dfs
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/windows/kertish-dfs.exe

echo ""
echo "Building Administrative command-line tool (v$RELEASE_VERSION)"
cd ../admin-tool
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/kertish-admin
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/kertish-admin
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/windows/kertish-admin.exe

echo ""
echo "Building Manager Node service executable (v$RELEASE_VERSION)"
cd ../manager-node
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/kertish-manager
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/kertish-manager

echo ""
echo "Building Head Node service executable (v$RELEASE_VERSION)"
cd ../head-node
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/kertish-head
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/kertish-head

echo ""
echo "Building Data Node service executable (v$RELEASE_VERSION)"
cd ../data-node
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/kertish-data
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/kertish-data
