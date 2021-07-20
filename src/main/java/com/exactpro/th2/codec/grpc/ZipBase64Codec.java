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
import java.util.Collection;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.zip.ZipEntry;
import java.util.zip.ZipInputStream;
import java.util.zip.ZipOutputStream;

public class ZipBase64Codec {
	
	public static final Charset CHARSET = StandardCharsets.UTF_8;
	
	public static String encode(File dir) throws IOException {
		if (!dir.isDirectory()) throw new IllegalArgumentException("'dir' does not point to directory");

		try (ByteArrayOutputStream byteOutput = new ByteArrayOutputStream();
			 ZipOutputStream zipOut = new ZipOutputStream(byteOutput)) {
			
			Collection<File> files;
			try (Stream<Path> fileStream = Files.list(dir.toPath())) {
				files = fileStream.map(Path::toFile).collect(Collectors.toList());
			}
			for (File file : files)
				zipFile(file, file.getName(), zipOut);
			
			zipOut.finish();
			return new String(Base64.getEncoder().encode(byteOutput.toByteArray()), CHARSET);
		}
	}
	
	public static void decode(String data, File dir) throws IOException {
		decode(data.getBytes(CHARSET), dir);
	}
	
	public static void decode(byte[] data, File dir) throws IOException {
		byte[] bytes = Base64.getDecoder().decode(data);
		
		try (ZipInputStream zipIn = new ZipInputStream(new ByteArrayInputStream(bytes))) {

			ZipEntry entry;
			
			while ((entry = zipIn.getNextEntry()) != null) {
				File newFile = newFile(dir, entry);
				if (entry.isDirectory()) {
					if (!newFile.isDirectory() && !newFile.mkdirs()) {
						throw new IOException("Failed to create directory " + newFile);
					}
				} else {
					// fix for Windows-created archives
					File parent = newFile.getParentFile();
					if (!parent.isDirectory() && !parent.mkdirs()) {
						throw new IOException("Failed to create directory " + parent);
					}
					
					// write file content
					Files.copy(zipIn, newFile.toPath(), StandardCopyOption.REPLACE_EXISTING);
				}
			}
		}
	}
	
	private static File newFile(File destinationDir, ZipEntry zipEntry) throws IOException {
		File destFile = new File(destinationDir, zipEntry.getName());
		
		String destDirPath = destinationDir.getCanonicalPath();
		String destFilePath = destFile.getCanonicalPath();
		
		if (!destFilePath.startsWith(destDirPath + File.separator)) {
			throw new IOException("Entry is outside of the target dir: " + zipEntry.getName());
		}
		
		return destFile;
	}
	
	private static void zipFile(File fileToZip, String fileName, ZipOutputStream zipOut) throws IOException {
        if (fileToZip.isHidden()) {
            return;
        }
        if (fileToZip.isDirectory()) {
            if (!fileName.endsWith("/")) {
            	fileName += "/";
            }
			zipOut.putNextEntry(new ZipEntry(fileName));
            zipOut.closeEntry();
            
            File[] children = fileToZip.listFiles();
            if (children == null)
            	return;
            for (File childFile : children) {
                zipFile(childFile, fileName + "/" + childFile.getName(), zipOut);
            }
            return;
        }
        ZipEntry zipEntry = new ZipEntry(fileName);
        zipOut.putNextEntry(zipEntry);
		Files.copy(fileToZip.toPath(), zipOut);
    }
}
