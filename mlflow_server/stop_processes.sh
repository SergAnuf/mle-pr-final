#!/bin/bash
ps aux | grep '[m]lflow' | awk '{print $2}' | xargs kill -9
ps aux | grep '[j]upyter' | awk '{print $2}' | xargs kill -9
docker stop
ps aux | grep '[a]irflow' | awk '{print $2}' | xargs kill -9