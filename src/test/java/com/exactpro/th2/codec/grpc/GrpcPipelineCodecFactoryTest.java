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

package com.exactpro.th2.codec.grpc;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.junit.jupiter.api.Test;

public class GrpcPipelineCodecFactoryTest {
	
	@Test
	public void initTest() throws IOException {
		
		Path protoDir = Path.of("src/test/proto");
		String encodedProtos = ZipBase64Codec.encode(protoDir.toFile());
		
		String parentDir = "/tmp/protos";
		InputStream inputStream = new ByteArrayInputStream(encodedProtos.getBytes(StandardCharsets.UTF_8));
		Path decodedProtosDir = GrpcPipelineCodecFactory.Companion.decodeProtos(inputStream, parentDir);

		try (Stream<Path> expected = Files.list(protoDir); Stream<Path> actual = Files.list(decodedProtosDir)) {
			Set<Path> expSet = expected.map(Path::getFileName).collect(Collectors.toSet());
			Set<Path> actSet = actual.map(Path::getFileName).collect(Collectors.toSet());

			assertEquals(expSet, actSet);
		}
	}
}
