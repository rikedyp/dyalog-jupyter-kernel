#!/bin/sh
set -e

BASEDIR=$(dirname "$0")

case $(uname) in
	Darwin)	KERNELDIR=~/Library/Jupyter/kernels ;;
	Linux)	KERNELDIR=~/.local/share/jupyter/kernels ;;
	*)	exit 1
esac
mkdir -p "$KERNELDIR"
cp -r "$BASEDIR"/dyalog-kernel "$KERNELDIR"/

SITEDIR=$(python3 -m site --user-site)
CONDIR=/home/$USER/anaconda3/lib/python3.7/site-packages
mkdir -p "$SITEDIR"
cp -r "$BASEDIR"/dyalog_kernel "$SITEDIR"/
cp -r "$BASEDIR"/dyalog_kernel "$CONDIR"/
