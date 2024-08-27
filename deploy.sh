#!/bin/bash

# Define the source directories
COMMON_SRC="common/src/common"
SILVER_INFRA="silver/infrastructure"
SILVER_TRANS="silver/transformations"
SILVER_YAML="silver/yaml_config"
SILVER_MAIN="silver/main.py"
GOLD_INFRA="gold/infrastructure"
GOLD_TRANS="gold/transformations"
GOLD_YAML="gold/yaml_config"
GOLD_MAIN="gold/main.py"

# Define the destination directories for the zip files and copied files
COMMON_DEST="../../infrastructure/jobs/common"
SILVER_DEST="../infrastructure/jobs/silver"
GOLD_DEST="../infrastructure/jobs/gold"

# Create the destination directories if they don't exist
mkdir -p "$COMMON_DEST"
mkdir -p "$SILVER_DEST"
mkdir -p "$GOLD_DEST"

# Logging function
log() {
    echo "$(date '+%Y-%m-%d %H:%M:%S') - $1"
}

# Zip the contents of each folder and move them to the respective jobs folder

# Common
log "Zipping $COMMON_SRC..."
cd "$(dirname "$COMMON_SRC")" || exit
zip -r "$COMMON_DEST/common.zip" "$(basename "$COMMON_SRC")"
log "Moved $COMMON_SRC to $COMMON_DEST/common.zip"
cd - > /dev/null || exit

# Silver
log "Zipping $SILVER_INFRA..."
cd "$(dirname "$SILVER_INFRA")" || exit
zip -r "$SILVER_DEST/infrastructure.zip" "$(basename "$SILVER_INFRA")"
log "Moved $SILVER_INFRA to $SILVER_DEST/infrastructure.zip"
cd - > /dev/null || exit

log "Zipping $SILVER_TRANS..."
cd "$(dirname "$SILVER_TRANS")" || exit
zip -r "$SILVER_DEST/transformations.zip" "$(basename "$SILVER_TRANS")"
log "Moved $SILVER_TRANS to $SILVER_DEST/transformations.zip"
cd - > /dev/null || exit

log "Copying $SILVER_YAML to $SILVER_DEST..."
cp -r "$SILVER_YAML" "$SILVER_DEST/yaml_config"
log "Copied $SILVER_YAML to $SILVER_DEST/yaml_config"

log "Copying $SILVER_MAIN to $SILVER_DEST..."
cp "$SILVER_MAIN" "$SILVER_DEST/main.py"
log "Copied $SILVER_MAIN to $SILVER_DEST/main.py"

# Gold
log "Zipping $GOLD_INFRA..."
cd "$(dirname "$GOLD_INFRA")" || exit
zip -r "$GOLD_DEST/infrastructure.zip" "$(basename "$GOLD_INFRA")"
log "Moved $GOLD_INFRA to $GOLD_DEST/infrastructure.zip"
cd - > /dev/null || exit

log "Zipping $GOLD_TRANS..."
cd "$(dirname "$GOLD_TRANS")" || exit
zip -r "$GOLD_DEST/transformations.zip" "$(basename "$GOLD_TRANS")"
log "Moved $GOLD_TRANS to $GOLD_DEST/transformations.zip"
cd - > /dev/null || exit

log "Copying $GOLD_YAML to $GOLD_DEST..."
cp -r "$GOLD_YAML" "$GOLD_DEST/yaml_config"
log "Copied $GOLD_YAML to $GOLD_DEST/yaml_config"

log "Copying $GOLD_MAIN to $GOLD_DEST..."
cp "$GOLD_MAIN" "$GOLD_DEST/main.py"
log "Copied $GOLD_MAIN to $GOLD_DEST/main.py"

log "Zipping and moving completed successfully!"
