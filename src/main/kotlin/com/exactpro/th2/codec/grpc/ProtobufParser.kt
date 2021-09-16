/*******************************************************************************
 * Copyright 2021 Exactpro (Exactpro Systems Limited)
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
import mu.KotlinLogging
import java.io.File
import java.nio.file.Files
import java.nio.file.Path


class ProtobufParser(private val protoCompileDirectory: Path) {

    companion object {
        val logger = KotlinLogging.logger { }
    }

    private data class ParsedProtoFile(
        val descriptor: List<DescriptorProtos.FileDescriptorProto>
    )

    private fun compileSchema(protoPaths: List<String>, protoFile: String, args: List<String>) {
        val exitCode = Protoc.runProtoc((protoPaths.map { "--proto_path=$it" } + args + listOf(
            protoFile
        )).toTypedArray())

        if (exitCode != 0) {
            throw ProtoParseException("Failed to generate schema for: $protoFile")
        }
    }

    private fun lookupProtos(
        protoPaths: List<String>, protoFile: String, tempDir: Path, resolved: MutableSet<String>
    ): List<DescriptorProtos.FileDescriptorProto> {
        val schema = generateSchema(protoPaths, protoFile, tempDir)
        return schema.fileList.filter { resolved.add(it.name) }.flatMap { fd ->
            fd.dependencyList.filterNot(resolved::contains)
                .flatMap { lookupProtos(protoPaths, it, tempDir, resolved) } + fd
        }
    }

    private fun generateSchema(protoPaths: List<String>, protoFile: String, tempDir: Path): DescriptorProtos.FileDescriptorSet {
        var outFile: File? = null
        return try {
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

    fun parseProtosToSchemas(protoDir: File, protoFiles: Collection<File>): List<ProtoSchema> {
        return protoFiles.map { file ->
            val descriptor = parseProtoFile(file, protoDir.path).descriptor
            if (logger.isDebugEnabled) {
                descriptor.forEach {
                    logger.debug { "Found descriptor: ${it.name}" }
                }
            }
            ProtoSchema(descriptor)
        }
    }

    private fun parseProtoFile(protoFile: File, searchPath: String): ParsedProtoFile {
        val fileDescription = lookupProtos(
            listOf(searchPath), protoFile.path, protoCompileDirectory, mutableSetOf()
        )
        return ParsedProtoFile(fileDescription)
    }
}
