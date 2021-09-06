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

package com.exactpro.th2.codec.grpc;

import java.io.File;
import java.nio.file.Path;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.google.protobuf.ByteString;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import org.apache.commons.io.FileUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ProtoDecoder {
	private static final Logger LOGGER = LoggerFactory.getLogger(ProtoDecoder.class);
	
	public static final String GRPC_CALL = "GRPC_CALL";
	private static final String TEMP_DIR = "gen/test/temp";

	private final Map<String, Descriptors.ServiceDescriptor> serviceDescriptorMap = new HashMap<>();
	
	public ProtoDecoder(Path protosPath) {
		Collection<File> protos = FileUtils.listFiles(protosPath.toFile(), new String[]{"proto"}, true);
		List<ProtoSchema> protoSchemas = new ProtobufParser(TEMP_DIR).parseProtosToSchemas(protosPath.toFile(), protos);
		protoSchemas.forEach(schema -> serviceDescriptorMap.putAll(schema.getServices()));
	}
	
	public Message decode(RawMessage message) throws InvalidProtocolBufferException, JsonProcessingException {
		RawMessageMetadata metadata = message.getMetadata();
		Map<String, String> props = metadata.getPropertiesMap();
		String grpcCall = props.get(GRPC_CALL);
		if (grpcCall == null) {
			throw new IllegalArgumentException(GRPC_CALL + " property is not set in the message metadata");
		}
		
		Direction direction = metadata.getId().getDirection();
		
		DynamicMessage dynamicMessage = decode(message.getBody(), grpcCall, direction);
		return ProtoUtils.toMessage(dynamicMessage);
	}
	
	public DynamicMessage decode(ByteString body, String grpcCall, Direction direction) throws InvalidProtocolBufferException, JsonProcessingException {
		Descriptors.Descriptor descriptor = getDescriptor(grpcCall, direction);
		return DynamicMessage.newBuilder(descriptor).mergeFrom(body).build();
	}
	
	private Descriptors.Descriptor getDescriptor(String grpcCallPath, Direction direction) {
		GrpcCall grpcCall = new GrpcCall(grpcCallPath);
		String serviceName = grpcCall.getService();

		LOGGER.debug("Getting descriptor for {}, direction: {}", grpcCall, direction);
		
		Descriptors.ServiceDescriptor serviceDesc = serviceDescriptorMap.get(serviceName);
		if (serviceDesc == null)
			throw new NoSuchElementException("There is no such service '" + serviceName + '\'');
		
		String methodName = grpcCall.getMethod();
		Descriptors.MethodDescriptor methodDesc = serviceDesc.findMethodByName(methodName);
		if (methodDesc == null)
			throw new NoSuchElementException("There is no such method '" + methodName + "' for service '" + serviceName + '\'');

		if (direction == Direction.FIRST)
			return methodDesc.getInputType();
		else
			return methodDesc.getOutputType();
	}
	
	private static class GrpcCall {
		private final String service;
		private final String method;
		
		public GrpcCall(String path) {
			Path callPath = Path.of(path);
			if (callPath.getNameCount() != 2)
				throw new IllegalArgumentException("'path' must consist of 2 elements");
			
			service = callPath.getName(0).toString();
			method = callPath.getName(1).toString();
		}
		
		public GrpcCall(String service, String method) {
			this.service = service;
			this.method = method;
		}
		
		public String getService() {
			return service;
		}
		
		public String getMethod() {
			return method;
		}
		
		@Override
		public String toString() {
			return "GrpcCall{" + "service='" + service + '\'' + ", method='" + method + '\'' + '}';
		}
	}
}
