#!/usr/bin/env python3
"""
Load XLSX files into DuckDB with separate tables for each sheet.
"""

import duckdb
import pandas as pd
import typer
from pathlib import Path
from typing import Optional
import sys


app = typer.Typer()


def sanitize_table_name(name: str) -> str:
    """Convert sheet name to valid DuckDB table name."""
    # Replace spaces, hyphens, and special chars with underscores
    sanitized = name.replace(" ", "_").replace("-", "_").replace(".", "_")
    # Remove other special characters
    sanitized = "".join(c for c in sanitized if c.isalnum() or c == "_")
    # Ensure it starts with letter or underscore
    if sanitized and sanitized[0].isdigit():
        sanitized = f"sheet_{sanitized}"
    return sanitized.lower()


@app.command()
def load_xlsx(
    xlsx_file: str = typer.Option(..., "--xlsx-file", "-f", help="Path to XLSX file to load"),
    duckdb_file: str = typer.Option(..., "--duckdb-file", "-d", help="Path to DuckDB database file"),
    table_prefix: str = typer.Option("", "--table-prefix", "-p", help="Prefix for table names"),
    skip_errors: bool = typer.Option(True, "--skip-errors/--no-skip-errors", help="Skip sheets that cause errors"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Load all sheets from an XLSX file into DuckDB as separate tables."""
    
    xlsx_path = Path(xlsx_file)
    if not xlsx_path.exists():
        typer.echo(f"Error: XLSX file not found: {xlsx_file}", err=True)
        raise typer.Exit(1)
    
    duckdb_path = Path(duckdb_file)
    duckdb_path.parent.mkdir(parents=True, exist_ok=True)
    
    try:
        # Read all sheets from XLSX
        if verbose:
            typer.echo(f"Reading sheets from: {xlsx_file}")
        
        # Get sheet names first
        xl_file = pd.ExcelFile(xlsx_file)
        sheet_names = xl_file.sheet_names
        
        if verbose:
            typer.echo(f"Found {len(sheet_names)} sheets: {', '.join(sheet_names)}")
        
        # Connect to DuckDB
        conn = duckdb.connect(str(duckdb_path))
        
        loaded_sheets = []
        skipped_sheets = []
        
        for sheet_name in sheet_names:
            try:
                if verbose:
                    typer.echo(f"Processing sheet: {sheet_name}")
                
                # Read the sheet
                df = pd.read_excel(xlsx_file, sheet_name=sheet_name)
                
                if df.empty:
                    if verbose:
                        typer.echo(f"  Skipping empty sheet: {sheet_name}")
                    skipped_sheets.append(f"{sheet_name} (empty)")
                    continue
                
                # Sanitize table name
                table_name = sanitize_table_name(sheet_name)
                if table_prefix:
                    table_name = f"{table_prefix}_{table_name}"
                
                # Create table in DuckDB
                conn.execute(f"CREATE OR REPLACE TABLE {table_name} AS SELECT * FROM df")
                
                row_count = len(df)
                col_count = len(df.columns)
                
                loaded_sheets.append((sheet_name, table_name, row_count, col_count))
                
                if verbose:
                    typer.echo(f"  ✓ Created table '{table_name}': {row_count} rows, {col_count} columns")
                
            except Exception as e:
                error_msg = f"{sheet_name} (error: {str(e)})"
                if skip_errors:
                    skipped_sheets.append(error_msg)
                    if verbose:
                        typer.echo(f"  ⚠ Skipped sheet '{sheet_name}': {str(e)}")
                else:
                    typer.echo(f"Error processing sheet '{sheet_name}': {str(e)}", err=True)
                    raise typer.Exit(1)
        
        conn.close()
        
        # Summary
        typer.echo(f"\n=== XLSX Loading Summary ===")
        typer.echo(f"File: {xlsx_file}")
        typer.echo(f"Database: {duckdb_file}")
        typer.echo(f"Loaded {len(loaded_sheets)} sheets successfully:")
        
        for sheet_name, table_name, rows, cols in loaded_sheets:
            typer.echo(f"  {sheet_name} → {table_name}: {rows} rows, {cols} columns")
        
        if skipped_sheets:
            typer.echo(f"\nSkipped {len(skipped_sheets)} sheets:")
            for skipped in skipped_sheets:
                typer.echo(f"  {skipped}")
        
    except Exception as e:
        typer.echo(f"Error: {str(e)}", err=True)
        raise typer.Exit(1)


@app.command()
def load_all_xlsx(
    input_dir: str = typer.Option(..., "--input-dir", "-i", help="Directory containing XLSX files"),
    duckdb_file: str = typer.Option(..., "--duckdb-file", "-d", help="Path to DuckDB database file"),
    file_pattern: str = typer.Option("*.xlsx", "--pattern", "-p", help="File pattern to match"),
    skip_errors: bool = typer.Option(True, "--skip-errors/--no-skip-errors", help="Skip files/sheets that cause errors"),
    verbose: bool = typer.Option(False, "--verbose", "-v", help="Verbose output"),
) -> None:
    """Load all XLSX files from a directory into DuckDB."""
    
    input_path = Path(input_dir)
    if not input_path.exists():
        typer.echo(f"Error: Input directory not found: {input_dir}", err=True)
        raise typer.Exit(1)
    
    xlsx_files = list(input_path.glob(file_pattern))
    if not xlsx_files:
        typer.echo(f"No XLSX files found matching pattern '{file_pattern}' in {input_dir}")
        return
    
    typer.echo(f"Found {len(xlsx_files)} XLSX files to process")
    
    for xlsx_file in xlsx_files:
        # Use filename (without extension and timestamp) as table prefix
        filename = xlsx_file.stem
        # Remove timestamp pattern like _20250919_085132 from filename
        import re
        filename = re.sub(r'_\d{8}_\d{6}', '', filename)
        table_prefix = filename.replace(" ", "_").replace("-", "_")
        
        typer.echo(f"\n--- Processing: {xlsx_file.name} ---")
        
        try:
            # Call load_xlsx for each file
            load_xlsx(
                xlsx_file=str(xlsx_file),
                duckdb_file=duckdb_file,
                table_prefix=table_prefix,
                skip_errors=skip_errors,
                verbose=verbose
            )
        except Exception as e:
            if skip_errors:
                typer.echo(f"⚠ Skipped file {xlsx_file.name}: {str(e)}")
            else:
                raise


if __name__ == "__main__":
    app()