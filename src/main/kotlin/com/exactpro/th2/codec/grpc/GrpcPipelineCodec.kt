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
import com.exactpro.th2.codec.grpc.GrpcPipelineCodecFactory.Companion.PROTOCOL
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.plusAssign
import com.google.protobuf.util.JsonFormat
import mu.KotlinLogging
import java.io.File

class GrpcPipelineCodec (protoDir: File) : IPipelineCodec {
    companion object {
        val logger = KotlinLogging.logger { }
        const val ERROR_TYPE_MESSAGE = "th2-codec-error"
        const val ERROR_CONTENT_FIELD = "content"
    }
    private val decoder: ProtoDecoder = ProtoDecoder(protoDir.toPath())
    private val printer = JsonFormat.printer().includingDefaultValueFields()

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

            val parsed = parseMessage(message.rawMessage)
            if (logger.isDebugEnabled)
                logger.debug { "Decoded message:\n${printer.print(parsed)}" }

            builder += parsed
        }
        return builder.build()
    }

    private fun parseMessage(rawMessage: RawMessage): Message {
        val metadata = rawMessage.metadata
        val parsedBuilder = Message.newBuilder()
        var parsedMessage: Message
        try {
            parsedMessage = decoder.decode(rawMessage)
        } catch (ex: Exception) {
            logger.error(ex) { "Cannot decode message from $rawMessage. Creating $ERROR_TYPE_MESSAGE message with description." }
            return rawMessage.toErrorMessage(ex, PROTOCOL).build()
        }
        parsedBuilder.mergeFrom(parsedMessage)
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

    private fun RawMessage.toErrorMessage(exception: Exception, protocol: String): Message.Builder = Message.newBuilder().apply {
        if (hasParentEventId()) {
            parentEventId = parentEventId
        }
        metadata = toMessageMetadataBuilder(protocol).setMessageType(ERROR_TYPE_MESSAGE).build()

        val content = buildString {
            var throwable: Throwable? = exception

            while (throwable != null) {
                append("Caused by: ${throwable.message}. ")
                throwable = throwable.cause
            }
        }
        putFields(ERROR_CONTENT_FIELD, Value.newBuilder().setSimpleValue(content).build())
    }

    private fun RawMessage.toMessageMetadataBuilder(protocol: String): MessageMetadata.Builder {
        return MessageMetadata.newBuilder()
            .setId(metadata.id)
            .setTimestamp(metadata.timestamp)
            .setProtocol(protocol)
            .putAllProperties(metadata.propertiesMap)
    }

    override fun encode(messageGroup: MessageGroup): MessageGroup {
        TODO("Not yet implemented")
    }
}