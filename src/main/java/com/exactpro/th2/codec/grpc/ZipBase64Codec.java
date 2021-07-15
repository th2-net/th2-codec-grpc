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

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.nio.charset.Charset;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardCopyOption;
import java.util.Base64;
import java.util.Iterator;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZipBase64Codec {
	
	public static final Charset CHARSET = StandardCharsets.UTF_8;
	
	public static String encode(File dir) throws IOException {
		if (!dir.isDirectory()) throw new IllegalArgumentException("'dir' does not point to directory");

		try (ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
			 ZipOutputStream zipOut = new ZipOutputStream(byteOutput);
			 Stream<Path> files = Files.list(dir.toPath())) {
			
			for (Iterator<Path> it = files.iterator(); it.hasNext(); ) {
				File file = it.next().toFile();
				if (file.isDirectory())
					continue;
				ZipEntry archiveEntry = new ZipEntry(file.getName());
				zipOut.putNextEntry(archiveEntry);
				Files.copy(file.toPath(), zipOut);
				zipOut.closeEntry();
			}
			zipOut.finish();
			return new String(Base64.getEncoder().encode(byteOutput.toByteArray()), CHARSET);
		}
	}
	
	public static void decode(byte[] data, File dir) throws IOException {
		byte[] bytes = Base64.getDecoder().decode(data);
		
		try (ZipInputStream tarIn = new ZipInputStream(new ByteArrayInputStream(bytes))) {

			ZipEntry entry;
			while ((entry = tarIn.getNextEntry()) != null) {
				String name = entry.getName();
				Path dirPath = dir.toPath();
				Files.createDirectories(dirPath);

				Path path = dirPath.resolve(name);

				Files.copy(tarIn, path, StandardCopyOption.REPLACE_EXISTING);
			}
		}
	}
	
	public static void decode(String data, File dir) throws IOException {
		decode(data.getBytes(CHARSET), dir);
	}
}
