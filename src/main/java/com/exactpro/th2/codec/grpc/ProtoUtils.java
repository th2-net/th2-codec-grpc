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

import java.util.List;
import java.util.Map;

import com.exactpro.th2.common.grpc.ListValue;
import com.exactpro.th2.common.grpc.Message;
import com.exactpro.th2.common.grpc.Value;
import com.google.protobuf.Descriptors;
import com.google.protobuf.DynamicMessage;
import org.jetbrains.annotations.NotNull;

public class ProtoUtils {
	public static Message toMessage(DynamicMessage dynamicMessage) {
		Message.Builder messageBuilder = Message.newBuilder();
		messageBuilder.getMetadataBuilder().setMessageType(dynamicMessage.getDescriptorForType().getName());
		
		for (Map.Entry<Descriptors.FieldDescriptor, Object> fieldEntry : dynamicMessage.getAllFields().entrySet()) {
			Descriptors.FieldDescriptor descriptor = fieldEntry.getKey();
			Object objectValue = fieldEntry.getValue();
			Value value = convertToValue(objectValue);
			
			messageBuilder.putFields(descriptor.getName(), value);
		}
		return messageBuilder.build();
	}

    private static Value convertToValue(Object fieldValue) {
        Value.Builder valueBuilder = Value.newBuilder();
        if (fieldValue instanceof com.google.protobuf.Message) {
            Message nestedMessage = convertComplex((com.google.protobuf.Message) fieldValue);
            valueBuilder.setMessageValue(nestedMessage);
        } else if (fieldValue instanceof List<?>) {
            ListValue listValue = convertToListValue(fieldValue);
            valueBuilder.setListValue(listValue);
        } else {
            valueBuilder.setSimpleValue(fieldValue.toString());
        }
        return valueBuilder.build();
    }

    @NotNull
    private static ListValue convertToListValue(Object fieldValue) {
        ListValue.Builder listBuilder = ListValue.newBuilder();
        var fieldList = (List<?>)fieldValue;
        if (!fieldList.isEmpty() && fieldList.get(0) instanceof com.google.protobuf.Message) {
            fieldList.forEach(message -> listBuilder.addValues(
                            Value.newBuilder()
                            .setMessageValue(convertComplex((com.google.protobuf.Message)message))
                            .build()
                    ));
        } else {
            fieldList.forEach(value -> listBuilder.addValues(
                            Value.newBuilder().setSimpleValue(value.toString()).build()
                    ));
        }
        return listBuilder.build();
    }

    private static Message convertComplex(com.google.protobuf.Message fieldValue) {
        Message.Builder messageBuilder = Message.newBuilder();
        for (Map.Entry<Descriptors.FieldDescriptor, Object> fieldEntry : fieldValue.getAllFields().entrySet()) {
			Descriptors.FieldDescriptor descriptor = fieldEntry.getKey();
			Object objectValue = fieldEntry.getValue();
			Value value = convertToValue(objectValue);
			
			messageBuilder.putFields(descriptor.getName(), value);
		}
        return messageBuilder.build();
    }
}
