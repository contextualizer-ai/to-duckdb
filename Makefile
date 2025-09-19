# MongoDB to DuckDB Migration Makefile
# Dumps collections starting with 'flattened_' from MongoDB to DuckDB

# Default configuration - can be overridden at runtime
MONGO_HOST ?= localhost
MONGO_PORT ?= 27017
MONGO_DB ?= gold_metadata_biosample_centric_20250915
MONGO_URI ?= mongodb://$(MONGO_HOST):$(MONGO_PORT)/$(MONGO_DB)

# Collection filtering
COLLECTION_PREFIX ?= flattened_
COLLECTION_PATTERN ?= $(COLLECTION_PREFIX)*

# Output configuration
OUTPUT_DIR ?= ./output
DUCKDB_FILE ?= $(OUTPUT_DIR)/collections.duckdb
XLSX_DUCKDB_FILE ?= $(OUTPUT_DIR)/xlsx.duckdb

# GOLD XLSX downloads
GOLD_BASE_URL ?= https://gold.jgi.doe.gov:443/download
XLSX_FILES = site_excel sra_biome_img_excel cv_excel ecosystempaths

# Create output directory
$(OUTPUT_DIR):
	mkdir -p $(OUTPUT_DIR)

# List all collections matching the pattern
list-collections:
	@echo "Collections matching '$(COLLECTION_PATTERN)' in database '$(MONGO_DB)':"
	@mongosh --quiet --eval "db.runCommand('listCollections').cursor.firstBatch.filter(c => c.name.startsWith('$(COLLECTION_PREFIX)')).forEach(c => print(c.name))" $(MONGO_URI)

# Get collections as a space-separated list for processing
get-collections:
	@mongosh --quiet --eval "db.runCommand('listCollections').cursor.firstBatch.filter(c => c.name.startsWith('$(COLLECTION_PREFIX)')).map(c => c.name).join(' ')" $(MONGO_URI)

# Export single collection to JSON
export-collection-json: $(OUTPUT_DIR)
	@if [ -z "$(COLLECTION)" ]; then \
		echo "Error: COLLECTION variable must be set"; \
		echo "Usage: make export-collection-json COLLECTION=collection_name"; \
		exit 1; \
	fi
	@echo "Exporting collection '$(COLLECTION)' to JSON..."
	mongoexport --uri="$(MONGO_URI)" \
		--collection="$(COLLECTION)" \
		--type=json \
		--out="$(OUTPUT_DIR)/$(COLLECTION).json"

# Load JSON into DuckDB table
json-to-duckdb: $(OUTPUT_DIR)
	@if [ -z "$(COLLECTION)" ]; then \
		echo "Error: COLLECTION variable must be set"; \
		echo "Usage: make json-to-duckdb COLLECTION=collection_name"; \
		exit 1; \
	fi
	@if [ ! -f "$(OUTPUT_DIR)/$(COLLECTION).json" ]; then \
		echo "Error: JSON file $(OUTPUT_DIR)/$(COLLECTION).json not found"; \
		echo "Run 'make export-collection-json COLLECTION=$(COLLECTION)' first"; \
		exit 1; \
	fi
	@echo "Loading $(COLLECTION).json into DuckDB table..."
	@table_name=$$(echo "$(COLLECTION)" | sed 's/\./_/g'); \
	duckdb "$(DUCKDB_FILE)" -c "CREATE OR REPLACE TABLE $$table_name AS SELECT * FROM read_json_auto('$(OUTPUT_DIR)/$(COLLECTION).json', sample_size=-1);"

# Process single collection (export + convert)
process-collection: export-collection-json json-to-duckdb

# Show summary of tables in DuckDB
show-summary:
	@if [ ! -f "$(DUCKDB_FILE)" ]; then \
		echo "DuckDB file not found: $(DUCKDB_FILE)"; \
		exit 1; \
	fi
	@echo "=== DuckDB Tables Summary ==="
	@echo "Database: $(DUCKDB_FILE)"
	@echo ""
	@duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT table_name || ': ' || (SELECT COUNT(*) FROM duckdb_tables() t2 WHERE t2.table_name = t1.table_name) || ' rows, ' || (SELECT COUNT(*) FROM pragma_table_info(t1.table_name)) || ' columns' FROM duckdb_tables() t1 ORDER BY table_name;" 2>/dev/null || \
	for table in $$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT table_name FROM duckdb_tables();" 2>/dev/null); do \
		row_count=$$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM $$table;" 2>/dev/null); \
		col_count=$$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM pragma_table_info('$$table');" 2>/dev/null); \
		echo "$$table: $$row_count rows, $$col_count columns"; \
	done

# Process all collections matching the pattern
dump-all: $(OUTPUT_DIR)
	@echo "Dumping all collections matching '$(COLLECTION_PATTERN)' from '$(MONGO_DB)'..."
	@collections=$$($(MAKE) get-collections 2>/dev/null); \
	if [ -z "$$collections" ]; then \
		echo "No collections found matching pattern '$(COLLECTION_PATTERN)'"; \
		exit 1; \
	fi; \
	echo "Found collections: $$collections"; \
	for collection in $$collections; do \
		echo "Processing collection: $$collection"; \
		$(MAKE) process-collection COLLECTION=$$collection; \
	done
	@echo "All collections processed successfully!"
	@echo ""
	$(MAKE) show-summary

# Clean up generated files
clean:
	rm -rf $(OUTPUT_DIR)

# Download single XLSX file
download-xlsx: $(OUTPUT_DIR)
	@if [ -z "$(MODE)" ]; then \
		echo "Error: MODE variable must be set"; \
		echo "Usage: make download-xlsx MODE=site_excel"; \
		echo "Available modes: $(XLSX_FILES)"; \
		exit 1; \
	fi
	@echo "Downloading XLSX file for mode: $(MODE)"
	@timestamp=$$(date +%Y%m%d_%H%M%S); \
	curl -L -o "$(OUTPUT_DIR)/gold_$(MODE)_$$timestamp.xlsx" "$(GOLD_BASE_URL)?mode=$(MODE)"
	@echo "Downloaded to: $(OUTPUT_DIR)/gold_$(MODE)_$$timestamp.xlsx"

# Download all XLSX files
download-all-xlsx: $(OUTPUT_DIR)
	@echo "Downloading all GOLD XLSX files..."
	@for mode in $(XLSX_FILES); do \
		echo "Downloading $$mode..."; \
		timestamp=$$(date +%Y%m%d_%H%M%S); \
		curl -L -o "$(OUTPUT_DIR)/gold_$${mode}_$$timestamp.xlsx" "$(GOLD_BASE_URL)?mode=$$mode"; \
		echo "Downloaded: $(OUTPUT_DIR)/gold_$${mode}_$$timestamp.xlsx"; \
		sleep 1; \
	done
	@echo "All XLSX files downloaded successfully!"

# Install Python dependencies
install-deps:
	@echo "Installing Python dependencies with uv..."
	uv sync

# Load single XLSX file into DuckDB
load-xlsx: install-deps
	@if [ -z "$(XLSX_FILE)" ]; then \
		echo "Error: XLSX_FILE variable must be set"; \
		echo "Usage: make load-xlsx XLSX_FILE=path/to/file.xlsx"; \
		exit 1; \
	fi
	@echo "Loading XLSX file into DuckDB: $(XLSX_FILE)"
	uv run xlsx-to-duckdb load-xlsx --xlsx-file "$(XLSX_FILE)" --duckdb-file "$(DUCKDB_FILE)" --verbose

# Load all XLSX files from output directory into separate database
load-all-xlsx: install-deps $(OUTPUT_DIR)
	@echo "Loading all XLSX files from $(OUTPUT_DIR) into separate DuckDB..."
	uv run xlsx-to-duckdb load-all-xlsx --input-dir "$(OUTPUT_DIR)" --duckdb-file "$(XLSX_DUCKDB_FILE)" --verbose

# Load all XLSX files into collections database (dangerous - will mix with MongoDB data)
load-xlsx-into-collections: install-deps $(OUTPUT_DIR)
	@echo "WARNING: This will add XLSX data to your MongoDB collections database!"
	@echo "Loading all XLSX files from $(OUTPUT_DIR) into $(DUCKDB_FILE)..."
	uv run xlsx-to-duckdb load-all-xlsx --input-dir "$(OUTPUT_DIR)" --duckdb-file "$(DUCKDB_FILE)" --verbose

# Download and load all XLSX files into separate database
download-and-load-xlsx: download-all-xlsx load-all-xlsx
	@echo "Download and load complete!"

# Show current configuration
show-config:
	@echo "Current Configuration:"
	@echo "  MONGO_URI: $(MONGO_URI)"
	@echo "  MONGO_DB: $(MONGO_DB)"
	@echo "  COLLECTION_PREFIX: $(COLLECTION_PREFIX)"
	@echo "  OUTPUT_DIR: $(OUTPUT_DIR)"
	@echo "  DUCKDB_FILE: $(DUCKDB_FILE)"
	@echo "  XLSX_DUCKDB_FILE: $(XLSX_DUCKDB_FILE)"
	@echo "  GOLD_BASE_URL: $(GOLD_BASE_URL)"
	@echo "  XLSX_FILES: $(XLSX_FILES)"

.PHONY: list-collections get-collections export-collection-json json-to-duckdb process-collection dump-all clean show-config show-summary download-xlsx download-all-xlsx install-deps load-xlsx load-all-xlsx download-and-load-xlsx