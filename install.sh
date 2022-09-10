#!/bin/bash
echo "Gonna install necessary libraries ..."
platform='unknown'
unamestr=$(uname)
if [[ "$unamestr" == 'Linux' ]]; then
   platform='linux'
elif [[ "$unamestr" == 'Darwin' ]]; then
   platform='macos'
else
   echo "Don't expect on other systems"
   exit
fi

echo "Downloading essential tools"
if [[ "$platform" == 'linux' ]]; then
  apt-get update
  apt-get install build-essential cmake git
else
  brew update
  brew upgrade
  brew install autoconf automake cmake libtool
fi

echo "Downloading libuv..."
if [[ "$platform" == 'linux' ]]; then
  pushd /tmp
  wget http://dist.libuv.org/dist/v1.14.0/libuv-v1.14.0.tar.gz
  tar xzf libuv-v1.14.0.tar.gz
  pushd libuv-v1.14.0
  sh autogen.sh
  ./configure
  make install
  popd
  popd
else
  brew install libuv
fi

echo "Downloading openssl..."
if [[ "$platform" == 'linux' ]]; then
  apt-get install libssl-dev
else
  brew install openssl
  brew link --force openssl 
fi

echo "Downloading clang-format..."
if [[ "$platform" == 'linux' ]]; then
  apt-get install clang-format
else
  brew install clang-format
fi

echo "Finished!"
