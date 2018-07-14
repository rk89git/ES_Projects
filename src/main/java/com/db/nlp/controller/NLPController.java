package com.db.nlp.controller;

import com.db.common.constants.Indexes;
import com.db.nlp.co.NLPCO;
import com.db.nlp.dto.Annotation;
import com.db.nlp.dto.EntityDTO;
import com.db.nlp.dto.EntityTextDTO;
import com.db.nlp.services.NLPService;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;
import org.codehaus.jackson.type.TypeReference;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestMethod;
import org.springframework.web.bind.annotation.ResponseBody;

import java.util.Map;

/**
 * Created by DB on 13-05-2017.
 */
@Controller
@RequestMapping("/nlp/v1")

public class NLPController {
    private static Logger log = LogManager.getLogger(NLPController.class);
    @Autowired
    NLPService nlpService;


    @RequestMapping(method = RequestMethod.POST, value = "/getEntity")
    @ResponseBody
    public  ResponseEntity<Object> getNLPTags(@RequestBody  NLPCO nlpco
    ) {
        HttpStatus status = HttpStatus.OK;
        try {
            log.info("getting NLP tags: " + nlpco);

          EntityTextDTO entityDTO=  nlpService.getNLPTags(nlpco);
            log.info("Got NLP Tags");
            return new ResponseEntity<Object>(entityDTO, status);
        } catch (Exception e) {
            log.error("ERROR: could not get NLP Tags", e);
            status = HttpStatus.BAD_REQUEST;
            return new ResponseEntity<Object>(e.getMessage(), status);
        }
    }


}
