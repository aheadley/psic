#!/usr/bin/env python

import psic
import sys

worker = psic.CisoWorker()

worker.decompress(sys.argv[1], sys.argv[2])
