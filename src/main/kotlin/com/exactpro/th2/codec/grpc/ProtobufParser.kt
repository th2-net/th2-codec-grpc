/*******************************************************************************
 * Copyright 2020-2021 Exactpro (Exactpro Systems Limited)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 ******************************************************************************/

package com.exactpro.th2.codec.grpc

import com.github.os72.protocjar.Protoc
import com.google.protobuf.DescriptorProtos
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import mu.KotlinLogging
import java.io.File
import java.io.IOException
import java.nio.file.Files
import java.nio.file.Path


class ProtobufParser(private val protoCompileDirectory: String) {

    companion object {
        val logger = KotlinLogging.logger { }
    }

    private val defaultPath = "src/main/resources/proto"
    private val temporaryFiles = createDirectory("temp")

    private data class ParsedProtoFile(
        val descriptor: List<DescriptorProtos.FileDescriptorProto>
    )

    private fun createDirectory(directoryName: String): File {
        return protoCompileDirectory.let {
            var directory = File(it + File.separator + directoryName)
            if (!createDirIfNotExist(directory)) {
                directory = File(defaultPath + File.separator + directoryName)
                if (!createDirIfNotExist(directory))
                    throw IOException("Failed to create directory: ${it + File.separator + directoryName} or directory: ${directory.path}")
            }
            directory
        }
    }

    private fun createDirIfNotExist(directory: File): Boolean {
        return directory.exists() || directory.mkdirs()
    }

    private suspend fun compileSchema(protoPaths: List<String>, protoFile: String, args: List<String>) {
        withContext(Dispatchers.IO) {
            val exitCode = Protoc.runProtoc((protoPaths.map { "--proto_path=$it" } + args + listOf(
                protoFile
            )).toTypedArray())

            if (exitCode != 0) {
                throw ProtoParseException("Failed to generate schema for: $protoFile")
            }
        }
    }

    private suspend fun lookupProtos(
        protoPaths: List<String>, protoFile: String, tempDir: Path, resolved: MutableSet<String>
    ): List<DescriptorProtos.FileDescriptorProto> {
        return withContext(Dispatchers.IO) {
            val schema = generateSchema(protoPaths, protoFile, tempDir)
            schema.fileList.filter { resolved.add(it.name) }.flatMap { fd ->
                fd.dependencyList.filterNot(resolved::contains)
                    .flatMap { lookupProtos(protoPaths, it, tempDir, resolved) } + fd
            }
        }
    }

    private suspend fun generateSchema(
        protoPaths: List<String>, protoFile: String, tempDir: Path
    ): DescriptorProtos.FileDescriptorSet {
        return withContext(Dispatchers.IO) {
            var outFile: File? = null
            try {
                outFile = File.createTempFile(tempDir.toString(), null, null)
                compileSchema(
                    protoPaths, protoFile, listOf(
                        "--include_std_types", "--descriptor_set_out=$outFile"
                    )
                )
                Files.newInputStream(outFile.toPath()).use { DescriptorProtos.FileDescriptorSet.parseFrom(it) }
            } finally {
                outFile?.delete()
            }
        }
    }

    fun parseProtosToSchemas(protoDir: File, protoFiles: List<File>): List<ProtoSchema> {
        return runBlocking {
            val protoSchemas = protoFiles.map { file ->
                ProtoSchema(
                    parseProtoFile(file, protoDir.path).descriptor,
                )
            }
            protoSchemas
        }
    }

    private suspend fun parseProtoFile(
        protoFile: File, searchPath: String
    ): ParsedProtoFile {
        val fileDescription = lookupProtos(
            listOf(searchPath), protoFile.path, temporaryFiles.toPath(), mutableSetOf()
        )
        return ParsedProtoFile(fileDescription)
    }
}
