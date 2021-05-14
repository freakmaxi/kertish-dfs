#!/bin/sh

echo "Pruning the old releases..."
rm -R releases

echo "Creating folders..."
mkdir releases
mkdir -p releases/linux/arm64
mkdir -p releases/linux/amd64
mkdir -p releases/macosx/arm64
mkdir -p releases/macosx/amd64
mkdir -p releases/windows/arm64
mkdir -p releases/windows/amd64

major=$(date +%y)
buildNo=`printf %04d $(expr $(expr $(date +%s) - $(gdate -d "Jun 13 2020" +%s)) / 345600)`
export RELEASE_VERSION="$major.2.$buildNo"

go clean -cache

echo ""
echo "Building FileSystem command-line tool (v$RELEASE_VERSION)"
cd ../../fs-tool
echo "  > compiling linux arm64 release"
GOOS=linux GOARCH=arm64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/arm64/krtfs
echo "  > compiling linux amd64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/amd64/krtfs
echo "  > compiling macosx arm64 release"
GOOS=darwin GOARCH=arm64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/arm64/krtfs
echo "  > compiling macosx amd64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/amd64/krtfs
echo "  > compiling windows amd64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/windows/amd64/krtfs.exe

echo ""
echo "Building Administrative command-line tool (v$RELEASE_VERSION)"
cd ../admin-tool
echo "  > compiling linux arm64 release"
GOOS=linux GOARCH=arm64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/arm64/krtadm
echo "  > compiling linux amd64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/amd64/krtadm
echo "  > compiling macosx arm64 release"
GOOS=darwin GOARCH=arm64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/arm64/krtadm
echo "  > compiling macosx amd64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/amd64/krtadm
echo "  > compiling windows amd64 release"
GOOS=windows GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/windows/amd64/krtadm.exe

echo ""
echo "Building Manager Node service executable (v$RELEASE_VERSION)"
cd ../manager-node
echo "  > compiling linux arm64 release"
GOOS=linux GOARCH=arm64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/arm64/kertish-manager
echo "  > compiling linux amd64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/amd64/kertish-manager
echo "  > compiling macosx arm64 release"
GOOS=darwin GOARCH=arm64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/arm64/kertish-manager
echo "  > compiling macosx amd64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/amd64/kertish-manager

echo ""
echo "Building Hook Plugins (v$RELEASE_VERSION)"
mkdir ../-build-/executable/releases/linux/arm64/hooks
mkdir ../-build-/executable/releases/linux/amd64/hooks
mkdir ../-build-/executable/releases/macosx/arm64/hooks
mkdir ../-build-/executable/releases/macosx/amd64/hooks

cd ../hook-providers
for dir in */; do
  cd $dir
  echo "  > compiling linux arm64 release"
  GOOS=linux GOARCH=arm64 go build -buildmode=plugin -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/executable/releases/linux/arm64/hooks
  echo "  > compiling linux amd64 release"
  GOOS=linux GOARCH=amd64 go build -buildmode=plugin -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/executable/releases/linux/amd64/hooks
  echo "  > compiling macosx arm64 release"
  GOOS=darwin GOARCH=arm64 go build -buildmode=plugin -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/executable/releases/macosx/arm64/hooks
  echo "  > compiling macosx amd64 release"
  GOOS=darwin GOARCH=amd64 go build -buildmode=plugin -ldflags "-X main.version=$RELEASE_VERSION" -o ../../-build-/executable/releases/macosx/amd64/hooks
  cd ..
done

echo ""
echo "Building Head Node service executable (v$RELEASE_VERSION)"
cd ../head-node
echo "  > compiling linux arm64 release"
GOOS=linux GOARCH=arm64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/arm64/kertish-head
echo "  > compiling linux amd64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/amd64/kertish-head
echo "  > compiling macosx arm64 release"
GOOS=darwin GOARCH=arm64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/arm64/kertish-head
echo "  > compiling macosx amd64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/amd64/kertish-head

echo ""
echo "Building Data Node service executable (v$RELEASE_VERSION)"
cd ../data-node
echo "  > compiling linux arm64 release"
GOOS=linux GOARCH=arm64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/arm64/kertish-data
echo "  > compiling linux amd64 release"
GOOS=linux GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/linux/amd64/kertish-data
echo "  > compiling macosx arm64 release"
GOOS=darwin GOARCH=arm64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/arm64/kertish-data
echo "  > compiling macosx amd64 release"
GOOS=darwin GOARCH=amd64 go build -ldflags "-X main.version=$RELEASE_VERSION" -o ../-build-/executable/releases/macosx/amd64/kertish-data
