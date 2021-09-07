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

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import mu.KotlinLogging
import java.util.*

data class ProtoSchema(
    val protoSchema: List<DescriptorProtos.FileDescriptorProto>
) {

    companion object {
        val logger = KotlinLogging.logger { }
    }

    private val protoSchemaMap = protoSchema.associateBy { it.name }
    val services = protoSchema.flatMap { createDescriptor(it).services }.map { it.fullName to it }.toMap()

    fun getServiceDescriptor(serviceName: String): Optional<Descriptors.ServiceDescriptor> {
        return Optional.ofNullable(services[serviceName])
    }

    private fun createFileDescriptor(
        protoDescriptor: DescriptorProtos.FileDescriptorProto,
        builtDependencies: MutableMap<String, Descriptors.FileDescriptor>
    ): Descriptors.FileDescriptor {
        val descriptor = Descriptors.FileDescriptor.buildFrom(
            protoDescriptor, if (protoDescriptor.dependencyCount == 0) {
                emptyArray()
            } else {
                protoDescriptor.dependencyList.mapNotNull { dependency ->
                    builtDependencies[dependency] ?: protoSchemaMap[dependency]?.let {
                        createFileDescriptor(it, builtDependencies)
                    }
                }.toTypedArray()
            }
        )
        builtDependencies[protoDescriptor.name] = descriptor
        return descriptor
    }

    private fun createDescriptor(schema: DescriptorProtos.FileDescriptorProto): Descriptors.FileDescriptor {
        return createFileDescriptor(schema, mutableMapOf())
    }
}
