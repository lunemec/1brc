#!/usr/bin/env bash
set -euo pipefail

classes=build/java-baseline/classes
source=third_party/java-generator/CalculateAverage_baseline.java
main_class="$classes/dev/morling/onebrc/CalculateAverage_baseline.class"

if [[ -f "$main_class" && "$main_class" -nt "$source" && "$main_class" -nt "$0" ]]; then
    exit 0
fi

mkdir -p "$classes"
javac --release 21 -d "$classes" "$source"
