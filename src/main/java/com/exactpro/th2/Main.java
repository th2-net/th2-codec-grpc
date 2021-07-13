package com.exactpro.th2;

import java.io.File;
import java.nio.file.Paths;
import java.util.List;

import com.exactpro.th2.common.grpc.Event;
import com.google.protobuf.DescriptorProtos;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import com.google.protobuf.InvalidProtocolBufferException;
import com.google.protobuf.Message;
import com.google.protobuf.util.JsonFormat;

public class Main {
	public static void main(String[] args) throws InvalidProtocolBufferException, Descriptors.DescriptorValidationException {
		
		List<DescriptorProtos.FileDescriptorProto> descs = new ProtobufParser("gen/classes")
				.protoToFileDescriptors(new File("common.proto"), Paths.get("src/main/resources/proto").toAbsolutePath().toString());
		
		
		DescriptorProtos.FileDescriptorProto commonFileDescriptorProto = descs.stream().filter(desc -> desc.getName().contains("common")).findFirst().get();
		DescriptorProtos.FileDescriptorProto timestampFileDescriptorProto = descs.stream().filter(desc -> desc.getName().contains("timestamp")).findFirst().get();
		
		
		Descriptors.FileDescriptor timestampFileDesc = Descriptors.FileDescriptor.buildFrom(timestampFileDescriptorProto, new Descriptors.FileDescriptor[0]);
		Descriptors.FileDescriptor commonFileDesc = Descriptors.FileDescriptor.buildFrom(commonFileDescriptorProto, new Descriptors.FileDescriptor[]{timestampFileDesc});
		
		Descriptors.Descriptor descriptor = commonFileDesc.findMessageTypeByName("Event");
		Message expectedEventMessage = createMessage();

		DynamicMessage dynamicMessage2 = DynamicMessage.newBuilder(descriptor)
				.mergeFrom(expectedEventMessage.toByteArray()).build();
		System.out.println(protoMessageToJson(dynamicMessage2));
	}
	
	private static String protoMessageToJson(DynamicMessage dynamicMessage) throws InvalidProtocolBufferException {
		JsonFormat.Printer printer = JsonFormat.printer().includingDefaultValueFields();
		return printer.print(dynamicMessage);
	}
	
	private static Message createMessage() {
		Event.Builder builder = Event.newBuilder();
		builder.setName("birthday");
		return builder.build();
	}
	
	private static DynamicMessage toDynamicMessage(Message proto) throws InvalidProtocolBufferException {
		DynamicMessage.Builder builder = DynamicMessage.newBuilder(Event.getDescriptor()).mergeFrom(proto);
		return builder.build();
	}
}
