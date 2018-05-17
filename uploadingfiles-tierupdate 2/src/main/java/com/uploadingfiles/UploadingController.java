package com.uploadingfiles;

import java.io.File;
import java.io.IOException;
import java.net.URISyntaxException;
import java.sql.SQLException;

import org.springframework.stereotype.Controller;
import org.springframework.ui.Model;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.multipart.MultipartFile;

@Controller
public class UploadingController {

    public static final String uploadingdir = System.getProperty("user.dir") + "/uploadingdir/";

    @RequestMapping("/")
    public String uploading(Model model) {
        File file = new File(uploadingdir);
        model.addAttribute("files", file.listFiles());
        return "uploading";
    }

    @RequestMapping(value = "/", method = RequestMethod.POST)
    public String uploadingPost(@RequestParam("uploadingFiles") MultipartFile[] uploadingFiles) throws Exception {
    	 
    	final File folder = new File(uploadingdir);
 		
         for (final File fileEntry : folder.listFiles()) {
         	fileEntry.delete();
         }
         
        for(MultipartFile uploadedFile : uploadingFiles) {
            File file = new File(uploadingdir + uploadedFile.getOriginalFilename());
            uploadedFile.transferTo(file);
        }
        Helper.listFilesForFolder(folder);
        return "redirect:/";
    }
    

   

}



     