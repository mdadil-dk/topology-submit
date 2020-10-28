package com.storm.test.SubmitTopology.controller;

import java.io.FileNotFoundException;
import java.io.IOException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

@org.springframework.web.bind.annotation.RestController
@RequestMapping(value = "/v1")
public class RestController {

  @Autowired
  private FileStorage fileStorage;

  @Autowired
  SubmitTopology submitTopology;

  private Logger LOGGER = LoggerFactory.getLogger(RestController.class);

  @PostMapping("/submit")
  public ResponseEntity<Object> readCSVFile(@RequestParam("name") String name,
      @RequestParam("file") MultipartFile file)
      throws FileNotFoundException, IOException, Exception {

    if (!file.isEmpty()) {

      String path = fileStorage.storeFile(file);
      LOGGER.info("Temp path: " + path);

      if (path != null && !path.isBlank()) {
        String msg = submitTopology.submit(name, path);
        return ResponseEntity.ok(msg);
      }
    }

    return ResponseEntity.badRequest().body("Invalid request");
  }

}
