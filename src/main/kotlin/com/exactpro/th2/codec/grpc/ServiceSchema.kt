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

import com.google.protobuf.Descriptors
import org.apache.commons.io.FileUtils
import java.io.File
import java.nio.file.Files
import java.util.function.Consumer

class ServiceSchema constructor(protoDir: File) {
    private var serviceDescriptorMap: Map<String, Descriptors.ServiceDescriptor> = HashMap()

    init {
        val protos = FileUtils.listFiles(protoDir, arrayOf("proto"), true)
        val schemaPath = File("schema")
        schemaPath.deleteRecursively()
        Files.createDirectory(schemaPath.toPath())
        val protoSchemas = ProtobufParser(schemaPath.toPath()).parseProtosToSchemas(protoDir, protos)
        protoSchemas.forEach(Consumer { schema: ProtoSchema -> serviceDescriptorMap += schema.services })
    }

    fun getServiceDesc(name: String): Descriptors.ServiceDescriptor {
        return serviceDescriptorMap[name] ?: throw IllegalArgumentException("There is no such service '$name'")
    }
}