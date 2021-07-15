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
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Optional;
import java.util.stream.Collectors;

import com.exactpro.th2.common.grpc.Direction;
import com.exactpro.th2.common.grpc.RawMessage;
import com.exactpro.th2.common.grpc.RawMessageMetadata;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.core.type.TypeReference;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.util.JsonFormat;

public class ProtoDecoder {
	private final List<ProtoSchema> protoSchemas;
	public static final String GRPC_CALL = "GRPC_CALL";
	private static final String TEMP_DIR = "gen/test/temp";
	private static final ObjectMapper OBJECT_MAPPER = new ObjectMapper();
	private static final TypeReference<Map<String, Object>> STRING_OBJECT_MAP_TYPE_REF = new TypeReference<>() {};
	
	public ProtoDecoder(Path protosPath) throws IOException {
		List<File> protos = Files.list(protosPath).map(Path::toFile).collect(Collectors.toList());
		protoSchemas = new ProtobufParser(TEMP_DIR).parseProtosToSchemas(protosPath.toFile(), protos);
	}
	
	public Map<String, Object> decode(RawMessage message) throws InvalidProtocolBufferException, JsonProcessingException {
		RawMessageMetadata metadata = message.getMetadata();
		String grpcCall = metadata.getPropertiesOrThrow(GRPC_CALL);
		Direction direction = metadata.getId().getDirection();
		
		Descriptors.Descriptor descriptor = getDescriptor(grpcCall, direction);
		DynamicMessage dynamicMessage = DynamicMessage.newBuilder(descriptor)
				.mergeFrom(message.getBody()).build();
		return decodeDynamicMessage(dynamicMessage);
	}
	
	private Descriptors.Descriptor getDescriptor(String grpcCallPath, Direction direction) {
		GrpcCall grpcCall = new GrpcCall(grpcCallPath);
		String serviceName = grpcCall.getService();
		//TODO make a map: name -> ServerDescriptor to avoid this loop being called constantly
		for (ProtoSchema schema : protoSchemas) {
			Optional<Descriptors.ServiceDescriptor> serviceDescOpt = schema.getServiceDescriptor(serviceName);
			if (serviceDescOpt.isEmpty())
				continue;
			
			String methodName = grpcCall.getMethod();
			Descriptors.MethodDescriptor methodDesc = serviceDescOpt.get().findMethodByName(methodName);
			if (direction == Direction.FIRST)
				return methodDesc.getInputType();
			else
				return methodDesc.getOutputType();
		}
		throw new NoSuchElementException("There is no such service '" + serviceName + '\'');
	}
	
	private static Map<String, Object> decodeDynamicMessage(DynamicMessage dynamicMessage)
			throws InvalidProtocolBufferException, JsonProcessingException {
		JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields();
		return OBJECT_MAPPER.readValue(printer.print(dynamicMessage), STRING_OBJECT_MAP_TYPE_REF);
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
	}
}
