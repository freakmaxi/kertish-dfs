#!/bin/sh

echo "Pruning the old releases..."
rm -R releases

echo "Creating folders..."
mkdir releases
mkdir releases/linux
mkdir releases/macosx
mkdir releases/windows

major=$(date +%y)
buildNo=$(($(date +%s)/345600))

export RELEASE_VERSION="$major.1.$buildNo"

echo ""
echo "Building FileSystem command-line tool (v$RELEASE_VERSION)"
cd ../fs-tool
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/linux/2020-dfs
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/macosx/2020-dfs
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/windows/2020-dfs.exe

echo ""
echo "Building Administrative command-line tool (v$RELEASE_VERSION)"
cd ../admin-tool
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/linux/2020-admin
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/macosx/2020-admin
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/windows/2020-admin.exe

echo ""
echo "Building Manager Node service executable (v$RELEASE_VERSION)"
cd ../manager-node/src
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/linux/2020-manager
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/macosx/2020-manager
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/windows/2020-manager.exe

echo ""
echo "Building Head Node service executable (v$RELEASE_VERSION)"
cd ../../head-node/src
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/linux/2020-head
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/macosx/2020-head
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/windows/2020-head.exe

echo ""
echo "Building Data Node service executable (v$RELEASE_VERSION)"
cd ../../data-node/src
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/linux/2020-data
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/macosx/2020-data
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/windows/2020-data.exe