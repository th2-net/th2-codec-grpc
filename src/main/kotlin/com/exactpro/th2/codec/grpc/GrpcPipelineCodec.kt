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

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.common.grpc.AnyMessage
import com.exactpro.th2.common.grpc.Message
import com.exactpro.th2.common.grpc.MessageGroup
import com.exactpro.th2.common.grpc.RawMessage
import com.exactpro.th2.common.message.addField
import com.exactpro.th2.common.message.plusAssign
import java.io.File

class GrpcPipelineCodec (protoDir: File) : IPipelineCodec {
    companion object {
        private const val PROTOCOL = "protobuf"
    }

    override val protocol: String
        get() = PROTOCOL

    private val decoder: ProtoDecoder = ProtoDecoder(protoDir.toPath())

    override fun decode(messageGroup: MessageGroup): MessageGroup {
        val messages = messageGroup.messagesList

        if (messages.isEmpty() || messages.none(AnyMessage::hasRawMessage)) {
            return messageGroup
        }

        val builder = MessageGroup.newBuilder()
        for (message in messages) {
            if (!message.hasRawMessage()) {
                builder.addMessages(message)
                continue
            }

            builder += parseMessage(message.rawMessage)
        }
        return builder.build()
    }

    private fun parseMessage(rawMessage: RawMessage): Message {
        val metadata = rawMessage.metadata
        val parsedBuilder = Message.newBuilder()

        val fieldsMap = decoder.decode(rawMessage)

        fieldsMap.forEach {
            // TODO make fields adding more realistic
            parsedBuilder.addField(it.key, it.value)
        }

        return parsedBuilder.apply {
            parentEventId = rawMessage.parentEventId
            metadataBuilder.apply {
                putAllProperties(metadata.propertiesMap)
                this.id = metadata.id
                this.timestamp = metadata.timestamp
                this.protocol = PROTOCOL
            }
        }.build()
    }

    override fun encode(messageGroup: MessageGroup): MessageGroup {
        TODO("Not yet implemented")
    }
}