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
import com.exactpro.th2.codec.api.IPipelineCodecFactory
import com.exactpro.th2.codec.api.IPipelineCodecSettings
import mu.KotlinLogging
import org.apache.commons.io.FileUtils
import java.io.File
import java.io.InputStream
import java.nio.file.Files
import java.nio.file.Path

class GrpcPipelineCodecFactory : IPipelineCodecFactory {
    override val settingsClass: Class<out IPipelineCodecSettings>
        get() = GrpcPipelineCodecSettings::class.java

    private lateinit var protoDir: File

    override fun createCodec(): IPipelineCodec {
        return GrpcPipelineCodec(protoDir)
    }

    override fun init(dictionary: InputStream, settings: IPipelineCodecSettings?) {
        protoDir = decodeProtos(dictionary, parentProtosDir, settings).toFile()
    }

    companion object {
        private val logger = KotlinLogging.logger { }
        const val parentProtosDir = "/tmp/protos"

        fun decodeProtos(dictionary: InputStream, parentDir: String, settings: IPipelineCodecSettings?): Path {
            return dictionary.use {
                val parentDirPath = Path.of(parentDir)
                Files.createDirectories(parentDirPath)
                val protoDir = Files.createTempDirectory(parentDirPath, "")

                ZipBase64Codec.decode(it.readAllBytes(), protoDir.toFile())

                logger.info {
                    "Decoded proto files: ${
                        FileUtils.listFiles(parentDirPath.toFile(), Array(1){"proto"}, true)
                            .map { file -> parentDirPath.relativize(file.toPath()) }.toList()
                    }"
                }
                protoDir
            }
        }
    }
}