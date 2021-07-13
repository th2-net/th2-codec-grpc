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

package com.exactpro.th2

import com.google.protobuf.DescriptorProtos
import com.google.protobuf.Descriptors
import mu.KotlinLogging
import java.util.*

data class ProtoSchema(
    val protoSchema: List<DescriptorProtos.FileDescriptorProto>
) {

    companion object {
        fun getMainSchema(
            protoFileName: String,
            protoSchema: List<DescriptorProtos.FileDescriptorProto>
        ): DescriptorProtos.FileDescriptorProto {
            return protoSchema.find { it.name == protoFileName }
                ?: throw ProtoParseException("Incorrect proto descriptor name: '$protoFileName'")
        }

        val logger = KotlinLogging.logger { }
    }

    private val protoSchemaMap = protoSchema.associateBy { it.name }

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

    fun getServiceDescriptor(serviceName: String): Optional<Descriptors.ServiceDescriptor> {
        return protoSchema.stream().map { desc -> createDescriptor(desc) }
            .flatMap { desc -> desc.services.stream()}.filter { serviceDesc -> serviceDesc.name == serviceName }.findFirst()
    }
}
