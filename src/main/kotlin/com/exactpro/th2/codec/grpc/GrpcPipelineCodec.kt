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

import com.exactpro.th2.codec.api.IPipelineCodec
import com.exactpro.th2.codec.grpc.GrpcPipelineCodecFactory.Companion.PROTOCOL
import com.exactpro.th2.codec.util.toDebugString
import com.exactpro.th2.common.grpc.*
import com.exactpro.th2.common.message.plusAssign
import com.google.protobuf.util.JsonFormat
import mu.KotlinLogging
import org.apache.commons.lang3.StringUtils

class GrpcPipelineCodec (serviceSchema: ServiceSchema) : IPipelineCodec {
    companion object {
        val logger = KotlinLogging.logger { }
        const val ERROR_TYPE_MESSAGE = "th2-codec-error"
        const val ERROR_CONTENT_FIELD = "content"
    }
    private val decoder: ProtoDecoder = ProtoDecoder(serviceSchema)
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
            val rawMessage = message.rawMessage
            val metadata = rawMessage.metadata
            val protocol = metadata.protocol

            if (StringUtils.isNotEmpty(protocol) && protocol != PROTOCOL) {
                if (logger.isDebugEnabled) {
                    logger.debug(
                        "Message protocol is '{}'. Could not decode the message: {}",
                        protocol,
                        JsonFormat.printer().omittingInsignificantWhitespace().print(metadata)
                    )
                }
                builder.addMessages(message)
                continue
            }
            if (logger.isDebugEnabled) {
                logger.debug("Decoding message: {}" + rawMessage.metadata.id.toDebugString())
            }
            val parsed = parseMessage(rawMessage)
            if (logger.isDebugEnabled) {
                logger.debug("Message decoded: {}" + parsed.metadata.id.toDebugString())
            }
            if (logger.isTraceEnabled) {
                logger.trace { "Raw message: \n${rawMessage.toDebugString()}" }
                logger.trace { "Decoded message:\n${parsed.toDebugString()}" }
            }

            builder += parsed
        }
        return builder.build()
    }

    private fun parseMessage(rawMessage: RawMessage): Message {
        val metadata = rawMessage.metadata
        val parsedBuilder: Message.Builder
        try {
            parsedBuilder = decoder.decode(rawMessage)
        } catch (ex: Exception) {
            logger.error(ex) { "Cannot decode message from $rawMessage. Creating $ERROR_TYPE_MESSAGE message with description." }
            return rawMessage.toErrorMessage(ex, metadata.protocol).build()
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