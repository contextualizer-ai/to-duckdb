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

# Show current configuration
show-config:
	@echo "Current Configuration:"
	@echo "  MONGO_URI: $(MONGO_URI)"
	@echo "  MONGO_DB: $(MONGO_DB)"
	@echo "  COLLECTION_PREFIX: $(COLLECTION_PREFIX)"
	@echo "  OUTPUT_DIR: $(OUTPUT_DIR)"
	@echo "  DUCKDB_FILE: $(DUCKDB_FILE)"

.PHONY: list-collections get-collections export-collection-json json-to-duckdb process-collection dump-all clean show-config show-summary