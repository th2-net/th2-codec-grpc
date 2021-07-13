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

package com.exactpro.th2;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.io.IOException;
import java.nio.file.Path;
import java.util.Map;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.exactpro.th2.hello.world.HelloRequest;
import com.github.os72.protocjar.Protoc;
import com.google.protobuf.Message;
import org.apache.commons.lang3.ArrayUtils;
import org.junit.jupiter.api.Test;

public class ProtoDecoderTest {
	
	@Test
	public void testDecode() throws IOException {
		ProtoDecoder decoder = new ProtoDecoder(Path.of("src/test/proto"));
		String nameParam = "myProtoDecoder";
		Message helloRequest = HelloRequest.newBuilder().setName(nameParam).build();
		RawMessageMetadata metadata = RawMessageMetadata.newBuilder()
				.putProperties(ProtoDecoder.GRPC_CALL, "/Greeter/SayHello")
				.setId(MessageID.newBuilder().setDirection(Direction.FIRST))
				.build();
		
		RawMessage rawMessage = RawMessage.newBuilder()
				.setBody(helloRequest.toByteString())
				.setMetadata(metadata)
				.build();
		
		Map<String, Object> fields = decoder.decode(rawMessage);
		assertEquals(nameParam, fields.get("name"));
	}
	
	@Test
	public void testProtoc() throws IOException, InterruptedException {
		String protoPath = "src/test/proto";
		String input = "helloWorld.proto";
		String descFile = "gen/test/proto";
		String[] args = { "--include_std_types", "--descriptor_set_out=" + descFile};

		var exitCode = Protoc.runProtoc(ArrayUtils.add(ArrayUtils.addAll(new String[]{"--proto_path=" + protoPath}, args), input));
		System.out.println("Exit code: " + exitCode);
	}
}
