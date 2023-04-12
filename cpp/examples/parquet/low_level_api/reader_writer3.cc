//
// Created by Letian Jiang on 2023/4/12.
//
// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements. See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership. The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License. You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied. See the License for the
// specific language governing permissions and limitations
// under the License.

#include "reader_writer.h"

#include <cassert>
#include <fstream>
#include <iostream>
#include <memory>

/*
 * This example describes writing and reading Parquet Files in C++ and serves as a
 * reference to the API.
 * The file contains all the physical data types supported by Parquet.
 * This example uses the RowGroupWriter API that supports writing RowGroups optimized for
 * memory consumption.
 **/

/* Parquet is a structured columnar file format
 * Parquet File = "Parquet data" + "Parquet Metadata"
 * "Parquet data" is simply a vector of RowGroups. Each RowGroup is a batch of rows in a
 * columnar layout
 * "Parquet Metadata" contains the "file schema" and attributes of the RowGroups and their
 * Columns
 * "file schema" is a tree where each node is either a primitive type (leaf nodes) or a
 * complex (nested) type (internal nodes)
 * For specific details, please refer the format here:
 * https://github.com/apache/parquet-format/blob/master/LogicalTypes.md
 **/

const char PARQUET_FILENAME[] = "parquet_nested_example.parquet";

int main(int argc, char** argv) {
    /**********************************************************************************
                               PARQUET WRITER EXAMPLE
    **********************************************************************************/
    // parquet::REQUIRED fields do not need definition and repetition level values
    // parquet::OPTIONAL fields require only definition level values
    // parquet::REPEATED fields require both definition and repetition level values
    try {
        // Create a local file output stream instance.
        using FileClass = ::arrow::io::FileOutputStream;
        std::shared_ptr<FileClass> out_file;
        PARQUET_ASSIGN_OR_THROW(out_file, FileClass::Open(PARQUET_FILENAME));

        // Setup the parquet schema
        std::shared_ptr<GroupNode> schema = SetupNestedSchema();

        // Add writer properties
        parquet::WriterProperties::Builder builder;
        builder.compression(parquet::Compression::UNCOMPRESSED);
        std::shared_ptr<parquet::WriterProperties> props = builder.build();

        // Create a ParquetFileWriter instance
        std::shared_ptr<parquet::ParquetFileWriter> file_writer =
                parquet::ParquetFileWriter::Open(out_file, schema, props);

        // Append a RowGroup with a specific number of rows.
        parquet::RowGroupWriter* rg_writer = file_writer->AppendRowGroup();

        // Write the Int32 column
        parquet::Int32Writer* int32_writer =
                static_cast<parquet::Int32Writer*>(rg_writer->NextColumn());

        std::vector<int16_t> def_levels{3, 2, 3, 3, 3};
        std::vector<int16_t> rep_levels{0, 1, 1, 1, 1};
        std::vector<int32_t> values{1, 2, 3, 4};

        uint8_t bitmap = 0b11101;
        std::vector<int32_t> spaced_values{1, -999, 2, 3, 4};
        int32_writer->WriteBatchSpaced(5, def_levels.data(), rep_levels.data(), &bitmap, 0, spaced_values.data());

        // Close the ParquetFileWriter
        file_writer->Close();

        // Write the bytes to file
        DCHECK(out_file->Close().ok());
    } catch (const std::exception& e) {
        std::cerr << "Parquet write error: " << e.what() << std::endl;
        return -1;
    }

    /**********************************************************************************
                               PARQUET READER EXAMPLE
    **********************************************************************************/

    try {
        // Create a ParquetReader instance
        std::unique_ptr<parquet::ParquetFileReader> parquet_reader =
                parquet::ParquetFileReader::OpenFile(PARQUET_FILENAME, false);

        // Get the File MetaData
        std::shared_ptr<parquet::FileMetaData> file_metadata = parquet_reader->metadata();

        // Get the number of RowGroups
        int num_row_groups = file_metadata->num_row_groups();
        assert(num_row_groups == 1);

        // Get the number of Columns
        int num_columns = file_metadata->num_columns();
        assert(num_columns == 1);

        std::shared_ptr<parquet::RowGroupReader> row_group_reader =
                parquet_reader->RowGroup(0);

        std::shared_ptr<parquet::ColumnReader> column_reader = row_group_reader->Column(0);
        parquet::Int32Reader* int32_reader =
                static_cast<parquet::Int32Reader*>(column_reader.get());

        std::vector<int16_t> def_levels(3);
        std::vector<int16_t> rep_levels(3);
        std::vector<int32_t> values(2);
        int64_t values_read;
        int64_t levels_read = int32_reader->ReadBatch(3, def_levels.data(), rep_levels.data(), values.data(), &values_read);
        assert(levels_read == 3);
        assert(values_read == 2);
    } catch (const std::exception& e) {
        std::cerr << "Parquet read error: " << e.what() << std::endl;
        return -1;
    }

    std::cout << "Parquet Writing and Reading Complete" << std::endl;

    return 0;
}
