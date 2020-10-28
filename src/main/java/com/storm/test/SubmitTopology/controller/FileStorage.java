package com.storm.test.SubmitTopology.controller;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;
import org.springframework.web.multipart.MultipartFile;

@Service
public class FileStorage {

  public String storeFile(MultipartFile file) throws FileNotFoundException, IOException, Exception {
    String fileName = StringUtils.cleanPath(file.getOriginalFilename());

    // Getting absolute path of the uploaded mapped with target directory
    final String location = System.getProperty("java.io.tmpdir");

    Path targetLocation = Paths.get(location).toAbsolutePath().normalize().resolve(fileName);
    if (targetLocation.toFile().exists()) {
      Files.copy(file.getInputStream(), targetLocation, StandardCopyOption.REPLACE_EXISTING);
    } else {
      Files.copy(file.getInputStream(), targetLocation, StandardCopyOption.REPLACE_EXISTING);
    }

    return targetLocation.toString();
  }

}
