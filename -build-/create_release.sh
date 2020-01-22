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
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/linux/kertish-dfs
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/macosx/kertish-dfs
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/windows/kertish-dfs.exe

echo ""
echo "Building Administrative command-line tool (v$RELEASE_VERSION)"
cd ../admin-tool
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/linux/kertish-admin
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/macosx/kertish-admin
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/releases/windows/kertish-admin.exe

echo ""
echo "Building Manager Node service executable (v$RELEASE_VERSION)"
cd ../manager-node/src
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/linux/kertish-manager
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/macosx/kertish-manager
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/windows/kertish-manager.exe

echo ""
echo "Building Head Node service executable (v$RELEASE_VERSION)"
cd ../../head-node/src
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/linux/kertish-head
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/macosx/kertish-head
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/windows/kertish-head.exe

echo ""
echo "Building Data Node service executable (v$RELEASE_VERSION)"
cd ../../data-node/src
echo "  > compiling linux x64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/linux/kertish-data
echo "  > compiling macosx x64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/macosx/kertish-data
echo "  > compiling windows x64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/releases/windows/kertish-data.exe