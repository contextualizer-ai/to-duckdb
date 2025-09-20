# MongoDB to DuckDB Migration Makefile
# Dumps collections starting with 'flattened_' from MongoDB to DuckDB

# Default configuration - can be overridden at runtime
MONGO_HOST ?= localhost
MONGO_PORT ?= 27017
MONGO_DB ?= gold_metadata_biosample_centric_20250915
NMDC_DB ?= nmdc_20250919
MONGO_URI ?= mongodb://$(MONGO_HOST):$(MONGO_PORT)/$(MONGO_DB)
NMDC_URI ?= mongodb://$(MONGO_HOST):$(MONGO_PORT)/$(NMDC_DB)

# Collection filtering
COLLECTION_PREFIX ?= flattened_
COLLECTION_PATTERN ?= $(COLLECTION_PREFIX)*

# Output configuration
OUTPUT_DIR ?= ./output
DATE_STAMP := $(shell date +%Y%m%d)
DUCKDB_FILE ?= $(OUTPUT_DIR)/gold-api-$(DATE_STAMP).db
NMDC_DUCKDB_FILE ?= $(OUTPUT_DIR)/nmdc-api-$(DATE_STAMP).db
XLSX_DUCKDB_FILE ?= $(OUTPUT_DIR)/gold-xlsx-$(DATE_STAMP).db

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
# Show summary of API database tables
show-api-summary:
	@if [ ! -f "$(DUCKDB_FILE)" ]; then \
		echo "API database file not found: $(DUCKDB_FILE)"; \
		exit 1; \
	fi
	@echo "=== API Database Summary ==="
	@echo "Database: $(DUCKDB_FILE)"
	@echo ""
	@for table in $$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT table_name FROM duckdb_tables();" 2>/dev/null); do \
		row_count=$$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM $$table;" 2>/dev/null); \
		col_count=$$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM pragma_table_info('$$table');" 2>/dev/null); \
		echo "$$table: $$row_count rows, $$col_count columns"; \
	done

# Show summary of XLSX database tables
show-xlsx-summary:
	@if [ ! -f "$(XLSX_DUCKDB_FILE)" ]; then \
		echo "XLSX database file not found: $(XLSX_DUCKDB_FILE)"; \
		exit 1; \
	fi
	@echo "=== XLSX Database Summary ==="
	@echo "Database: $(XLSX_DUCKDB_FILE)"
	@echo ""
	@for table in $$(duckdb "$(XLSX_DUCKDB_FILE)" -noheader -list -c "SELECT table_name FROM duckdb_tables();" 2>/dev/null); do \
		row_count=$$(duckdb "$(XLSX_DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM $$table;" 2>/dev/null); \
		col_count=$$(duckdb "$(XLSX_DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM pragma_table_info('$$table');" 2>/dev/null); \
		echo "$$table: $$row_count rows, $$col_count columns"; \
	done

# Dump all collections to JSON only
dump-json: $(OUTPUT_DIR)
	@echo "Dumping all collections matching '$(COLLECTION_PATTERN)' to JSON..."
	@collections=$$($(MAKE) get-collections 2>/dev/null); \
	if [ -z "$$collections" ]; then \
		echo "No collections found matching pattern '$(COLLECTION_PATTERN)'"; \
		exit 1; \
	fi; \
	echo "Found collections: $$collections"; \
	for collection in $$collections; do \
		echo "Exporting collection: $$collection"; \
		$(MAKE) export-collection-json COLLECTION=$$collection; \
	done
	@echo "All collections exported to JSON!"

# Create API database from existing JSON files
make-api-db: $(OUTPUT_DIR)
	@echo "Creating API database from JSON files..."
	@json_files=$$(ls $(OUTPUT_DIR)/*.json 2>/dev/null || true); \
	if [ -z "$$json_files" ]; then \
		echo "No JSON files found. Run 'make dump-json' first."; \
		exit 1; \
	fi; \
	for json_file in $$json_files; do \
		collection=$$(basename $$json_file .json); \
		echo "Loading $$collection into DuckDB..."; \
		$(MAKE) json-to-duckdb COLLECTION=$$collection; \
	done
	@echo "API database created successfully!"
	@echo ""
	@for table in $$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT table_name FROM duckdb_tables();" 2>/dev/null); do \
		row_count=$$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM $$table;" 2>/dev/null); \
		col_count=$$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM pragma_table_info('$$table');" 2>/dev/null); \
		echo "$$table: $$row_count rows, $$col_count columns"; \
	done

# Dump NMDC collections to JSON only
dump-nmdc-json: $(OUTPUT_DIR)
	@echo "Dumping NMDC collections matching '$(COLLECTION_PATTERN)' to JSON..."
	@collections=$$(mongosh --quiet --eval "db.runCommand('listCollections').cursor.firstBatch.filter(c => c.name.startsWith('$(COLLECTION_PREFIX)')).map(c => c.name).join(' ')" $(NMDC_URI) 2>/dev/null); \
	if [ -z "$$collections" ]; then \
		echo "No collections found matching pattern '$(COLLECTION_PATTERN)' in $(NMDC_DB)"; \
		exit 1; \
	fi; \
	echo "Found collections: $$collections"; \
	for collection in $$collections; do \
		echo "Exporting collection: $$collection"; \
		mongoexport --uri="$(NMDC_URI)" --collection="$$collection" --type=json --out="$(OUTPUT_DIR)/nmdc_$$collection.json"; \
	done
	@echo "All NMDC collections exported to JSON!"

# Create NMDC API database from existing JSON files
make-nmdc-api-db: $(OUTPUT_DIR)
	@echo "Creating NMDC API database from JSON files..."
	@json_files=$$(ls $(OUTPUT_DIR)/nmdc_*.json 2>/dev/null || true); \
	if [ -z "$$json_files" ]; then \
		echo "No NMDC JSON files found. Run 'make dump-nmdc-json' first."; \
		exit 1; \
	fi; \
	for json_file in $$json_files; do \
		collection=$$(basename $$json_file .json | sed 's/^nmdc_//'); \
		echo "Loading $$collection into NMDC DuckDB..."; \
		table_name=$$(echo "$$collection" | sed 's/\./_/g'); \
		duckdb "$(NMDC_DUCKDB_FILE)" -c "CREATE OR REPLACE TABLE $$table_name AS SELECT * FROM read_json_auto('$$json_file', sample_size=-1);"; \
	done
	@echo "NMDC API database created successfully!"
	@echo ""
	@for table in $$(duckdb "$(NMDC_DUCKDB_FILE)" -noheader -list -c "SELECT table_name FROM duckdb_tables();" 2>/dev/null); do \
		row_count=$$(duckdb "$(NMDC_DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM $$table;" 2>/dev/null); \
		col_count=$$(duckdb "$(NMDC_DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM pragma_table_info('$$table');" 2>/dev/null); \
		echo "$$table: $$row_count rows, $$col_count columns"; \
	done

# Process all NMDC collections (JSON + DuckDB)
make-nmdc-database: $(OUTPUT_DIR)
	@echo "Dumping NMDC collections matching '$(COLLECTION_PATTERN)' to JSON..."
	@collections=$$(mongosh --quiet --eval "db.runCommand('listCollections').cursor.firstBatch.filter(c => c.name.startsWith('$(COLLECTION_PREFIX)')).map(c => c.name).join(' ')" $(NMDC_URI) 2>/dev/null); \
	if [ -z "$$collections" ]; then \
		echo "No collections found matching pattern '$(COLLECTION_PATTERN)' in $(NMDC_DB)"; \
		exit 1; \
	fi; \
	echo "Found collections: $$collections"; \
	for collection in $$collections; do \
		echo "Exporting collection: $$collection"; \
		mongoexport --uri="$(NMDC_URI)" --collection="$$collection" --type=json --out="$(OUTPUT_DIR)/nmdc_$$collection.json"; \
	done
	@echo "All NMDC collections exported to JSON!"
	@echo "Creating NMDC API database from JSON files..."
	@json_files=$$(ls $(OUTPUT_DIR)/nmdc_*.json 2>/dev/null || true); \
	for json_file in $$json_files; do \
		collection=$$(basename $$json_file .json | sed 's/^nmdc_//'); \
		echo "Loading $$collection into NMDC DuckDB..."; \
		table_name=$$(echo "$$collection" | sed 's/\./_/g'); \
		duckdb "$(NMDC_DUCKDB_FILE)" -c "CREATE OR REPLACE TABLE $$table_name AS SELECT * FROM read_json_auto('$$json_file', sample_size=-1);"; \
	done
	@echo "NMDC API database created successfully!"
	@echo ""
	@for table in $$(duckdb "$(NMDC_DUCKDB_FILE)" -noheader -list -c "SELECT table_name FROM duckdb_tables();" 2>/dev/null); do \
		row_count=$$(duckdb "$(NMDC_DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM $$table;" 2>/dev/null); \
		col_count=$$(duckdb "$(NMDC_DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM pragma_table_info('$$table');" 2>/dev/null); \
		echo "$$table: $$row_count rows, $$col_count columns"; \
	done

# Show summary of NMDC database tables
show-nmdc-summary:
	@if [ ! -f "$(NMDC_DUCKDB_FILE)" ]; then \
		echo "NMDC database file not found: $(NMDC_DUCKDB_FILE)"; \
		exit 1; \
	fi
	@echo "=== NMDC Database Summary ==="
	@echo "Database: $(NMDC_DUCKDB_FILE)"
	@echo ""
	@for table in $$(duckdb "$(NMDC_DUCKDB_FILE)" -noheader -list -c "SELECT table_name FROM duckdb_tables();" 2>/dev/null); do \
		row_count=$$(duckdb "$(NMDC_DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM $$table;" 2>/dev/null); \
		col_count=$$(duckdb "$(NMDC_DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM pragma_table_info('$$table');" 2>/dev/null); \
		echo "$$table: $$row_count rows, $$col_count columns"; \
	done

# Download XLSX files only
download-xlsx-files: $(OUTPUT_DIR)
	@echo "Downloading all GOLD XLSX files..."
	@for mode in $(XLSX_FILES); do \
		echo "Downloading $$mode..."; \
		timestamp=$$(date +%Y%m%d_%H%M%S); \
		curl -L -o "$(OUTPUT_DIR)/gold_$${mode}_$$timestamp.xlsx" "$(GOLD_BASE_URL)?mode=$$mode"; \
		echo "Downloaded: $(OUTPUT_DIR)/gold_$${mode}_$$timestamp.xlsx"; \
		sleep 1; \
	done
	@echo "All XLSX files downloaded successfully!"

# Create XLSX database from existing XLSX files
make-xlsx-db: install-deps $(OUTPUT_DIR)
	@echo "Loading all XLSX files from $(OUTPUT_DIR) into separate DuckDB..."
	uv run xlsx-to-duckdb load-all-xlsx --input-dir "$(OUTPUT_DIR)" --duckdb-file "$(XLSX_DUCKDB_FILE)" --verbose

# Process all collections matching the pattern (JSON + DuckDB)
make-api-database: $(OUTPUT_DIR)
	@echo "Dumping all collections matching '$(COLLECTION_PATTERN)' to JSON..."
	@collections=$$(mongosh --quiet --eval "db.runCommand('listCollections').cursor.firstBatch.filter(c => c.name.startsWith('$(COLLECTION_PREFIX)')).map(c => c.name).join(' ')" $(MONGO_URI) 2>/dev/null); \
	if [ -z "$$collections" ]; then \
		echo "No collections found matching pattern '$(COLLECTION_PATTERN)'"; \
		exit 1; \
	fi; \
	echo "Found collections: $$collections"; \
	for collection in $$collections; do \
		echo "Exporting collection: $$collection"; \
		mongoexport --uri="$(MONGO_URI)" --collection="$$collection" --type=json --out="$(OUTPUT_DIR)/$$collection.json"; \
	done
	@echo "All collections exported to JSON!"
	@echo "Creating API database from JSON files..."
	@json_files=$$(ls $(OUTPUT_DIR)/*.json 2>/dev/null || true); \
	for json_file in $$json_files; do \
		collection=$$(basename $$json_file .json); \
		echo "Loading $$collection into DuckDB..."; \
		table_name=$$(echo "$$collection" | sed 's/\./_/g'); \
		duckdb "$(DUCKDB_FILE)" -c "CREATE OR REPLACE TABLE $$table_name AS SELECT * FROM read_json_auto('$$json_file', sample_size=-1);"; \
	done
	@echo "API database created successfully!"
	@echo ""
	@for table in $$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT table_name FROM duckdb_tables();" 2>/dev/null); do \
		row_count=$$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM $$table;" 2>/dev/null); \
		col_count=$$(duckdb "$(DUCKDB_FILE)" -noheader -list -c "SELECT COUNT(*) FROM pragma_table_info('$$table');" 2>/dev/null); \
		echo "$$table: $$row_count rows, $$col_count columns"; \
	done

# Clean up JSON dumps from MongoDB
clean-json:
	@echo "Removing MongoDB JSON dumps..."
	rm -f $(OUTPUT_DIR)/*.json
	@echo "JSON dumps removed."

# Clean up NMDC JSON dumps
clean-nmdc-json:
	@echo "Removing NMDC JSON dumps..."
	rm -f $(OUTPUT_DIR)/nmdc_*.json
	@echo "NMDC JSON dumps removed."

# Clean up MongoDB DuckDB database
clean-api-db:
	@echo "Removing MongoDB API database..."
	rm -f $(OUTPUT_DIR)/gold-api-*.db
	@echo "API database removed."

# Clean up NMDC DuckDB database
clean-nmdc-db:
	@echo "Removing NMDC database..."
	rm -f $(OUTPUT_DIR)/nmdc-api-*.db
	@echo "NMDC database removed."

# Clean up XLSX downloads
clean-xlsx:
	@echo "Removing XLSX downloads..."
	rm -f $(OUTPUT_DIR)/*.xlsx
	@echo "XLSX downloads removed."

# Clean up XLSX DuckDB database
clean-xlsx-db:
	@echo "Removing XLSX database..."
	rm -f $(OUTPUT_DIR)/gold-xlsx-*.db
	@echo "XLSX database removed."

# Clean up all generated files
clean: clean-json clean-nmdc-json clean-api-db clean-nmdc-db clean-xlsx clean-xlsx-db
	@echo "Removing output directory if empty..."
	@rmdir $(OUTPUT_DIR) 2>/dev/null || true
	@echo "Cleanup complete."

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
	@echo "  NMDC_DUCKDB_FILE: $(NMDC_DUCKDB_FILE)"
	@echo "  XLSX_DUCKDB_FILE: $(XLSX_DUCKDB_FILE)"
	@echo "  GOLD_BASE_URL: $(GOLD_BASE_URL)"
	@echo "  XLSX_FILES: $(XLSX_FILES)"

.PHONY: list-collections get-collections export-collection-json json-to-duckdb process-collection dump-json make-api-db dump-nmdc-json make-nmdc-api-db make-nmdc-database show-nmdc-summary download-xlsx-files make-xlsx-db make-api-database clean clean-json clean-nmdc-json clean-api-db clean-nmdc-db clean-xlsx clean-xlsx-db show-config show-api-summary show-xlsx-summary download-xlsx download-all-xlsx install-deps load-xlsx load-all-xlsx download-and-load-xlsx