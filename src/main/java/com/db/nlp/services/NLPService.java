package com.db.nlp.services;

import com.db.nlp.co.NLPCO;
import com.db.nlp.dto.EntityDTO;
import com.db.nlp.dto.EntityTextDTO;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Scope;
import org.springframework.stereotype.Service;

/**
 * Created by DB on 13-05-2017.
 */
@Service

public class NLPService {

    @Autowired
    NLPServiceHandler nlpServiceHandler;

    public EntityTextDTO getNLPTags(NLPCO nlpco) {

        return nlpServiceHandler.getNLPTags(nlpco);

    }

}
