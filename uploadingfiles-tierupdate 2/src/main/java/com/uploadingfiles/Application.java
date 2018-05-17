package com.uploadingfiles;

import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;


//import com.test.HelloExample;

import java.io.File;
import java.io.IOException;

@SpringBootApplication
public class Application {

    public static void main(String[] args) throws IOException {

        new File(UploadingController.uploadingdir).mkdirs();
        SpringApplication.run(Application.class, args);
    }

		
		
}