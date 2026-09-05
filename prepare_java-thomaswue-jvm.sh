#!/usr/bin/env bash
set -euo pipefail

classes=build/java-thomaswue/classes
mkdir -p "$classes"
javac --enable-preview --release 21 \
    -d "$classes" \
    third_party/java-thomaswue/CalculateAverage_thomaswue.java
