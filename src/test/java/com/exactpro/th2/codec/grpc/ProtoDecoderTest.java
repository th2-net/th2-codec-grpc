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

import java.io.File;
import java.io.IOException;
import java.nio.file.Path;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.MessageID;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import org.junit.jupiter.api.Test;

public class ProtoDecoderTest {
	
	@Test
	public void testDecode() throws IOException {
		ServiceSchema serviceSchema = new ServiceSchema(new File("src/test/proto"));
		ProtoDecoder decoder = new ProtoDecoder(serviceSchema);

		String stringField = "stringFieldValue";
		String repeatedValue1 = "repeatedValue1";
		String repeatedValue2 = "repeatedValue2";

		String mapKey1 = "mapKey1";
		String mapKey2 = "mapKey2";
		String mapValue1 = "mapValue1";
		String mapValue2 = "mapValue2";

		TestRequest testRequest = TestRequest.newBuilder()
				.setStringField(stringField)
				.addRepeatedField("repeatedValue1")
				.addRepeatedField("repeatedValue2")
				.putMapField("mapKey1", "mapValue1")
				.putMapField("mapKey2", "mapValue2")
				.build();
		RawMessageMetadata metadata = RawMessageMetadata.newBuilder()
				.putProperties(ProtoDecoder.GRPC_CALL, "/com.exactpro.th2.codec.grpc.TestService/TestMethod")
				.setId(MessageID.newBuilder().setDirection(Direction.SECOND))
				.build();

		RawMessage rawMessage = RawMessage.newBuilder()
				.setBody(testRequest.toByteString())
				.setMetadata(metadata)
				.build();

		Message message = decoder.decode(rawMessage).build();

		assertEquals(stringField, message.getFieldsMap().get("stringField").getSimpleValue());
		
		assertEquals(repeatedValue1, message.getFieldsMap().get("repeatedField").getListValue().getValues(0).getSimpleValue());
		assertEquals(repeatedValue2, message.getFieldsMap().get("repeatedField").getListValue().getValues(1).getSimpleValue());
		
		assertEquals(mapValue1, message.getFieldsMap().get("mapField").getListValue().getValues(0).getMessageValue().getFieldsMap().get("value").getSimpleValue());
		assertEquals(mapValue2, message.getFieldsMap().get("mapField").getListValue().getValues(1).getMessageValue().getFieldsMap().get("value").getSimpleValue());
	}
}
