package com.db.nlp.services;

import java.io.BufferedOutputStream;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.logging.log4j.LogManager;import org.apache.logging.log4j.Logger;

import com.db.common.utils.DBConfig;
import com.db.nlp.model.NLPResult;
import com.db.nlp.model.NLPResult.Entity;

import opennlp.tools.cmdline.parser.ParserTool;
import opennlp.tools.namefind.NameFinderME;
import opennlp.tools.namefind.NameSample;
import opennlp.tools.namefind.NameSampleDataStream;
import opennlp.tools.namefind.TokenNameFinderModel;
import opennlp.tools.parser.Parse;
import opennlp.tools.parser.Parser;
import opennlp.tools.parser.ParserFactory;
import opennlp.tools.parser.ParserModel;
import opennlp.tools.util.ObjectStream;
import opennlp.tools.util.PlainTextByLineStream;
import opennlp.tools.util.Span;
import opennlp.tools.util.TrainingParameters;

public class NLPParser {
	{
		try {
			String modelsPath = config.getString("nlp.models.path");
			InputStream is = new FileInputStream(modelsPath+"en-parser-chunking.bin");
			ParserModel model = new ParserModel(is);
			parser = ParserFactory.create(model);
			InputStream isP = new FileInputStream(modelsPath+"en-ner-person.bin");
			InputStream isL = new FileInputStream(modelsPath+"en-ner-location.bin");
			InputStream isO = new FileInputStream(modelsPath+"en-ner-organization.bin");

			TokenNameFinderModel modelP = new TokenNameFinderModel(isP);
			TokenNameFinderModel modelL = new TokenNameFinderModel(isL);
			TokenNameFinderModel modelO = new TokenNameFinderModel(isO);

			isP.close();
			isL.close();
			isO.close();

			nameFinderP = new NameFinderME(modelP);
			nameFinderL = new NameFinderME(modelL);
			nameFinderO = new NameFinderME(modelO);
			/*NameFinderME nameFinderMe = new NameFinderME(modelL);
			nameFinderMe.train("en", arg1, arg2, arg3, arg4)*/

		} catch (Exception e) {
			log.error("Error occured in initializing NLPParser", e);
		}
	}

	private static Parser parser = null;
	private static NameFinderME nameFinderP = null;
	private static NameFinderME nameFinderL = null;
	private static NameFinderME nameFinderO = null;
	private static Logger log = LogManager.getLogger(NLPParser.class);
	private static DBConfig config = DBConfig.getInstance();


	private static NLPParser nlpParser = null;

	public static NLPParser getInstance() {
		if (nlpParser == null) {
			nlpParser = new NLPParser();
		}
		return nlpParser;
	}

	private List<String> getNounPhrases(Parse p, List<String> nounPhrases) {
		if (p.getType().equals("NN") || p.getType().equals("NNS") || p.getType().equals("NNP")
				|| p.getType().equals("NNPS")) {
			nounPhrases.add(p.getCoveredText());
		}

		for (Parse child : p.getChildren()) {
			getNounPhrases(child, nounPhrases);
		}

		return nounPhrases;
	}

	public synchronized List<String> getNouns(String line) throws Exception {

		Parse topParses[] = ParserTool.parseLine(line, parser, 1);
		List<String> nounPhrases = new ArrayList<String>();
		for (Parse p : topParses) {
			p.show();
			getNounPhrases(p, nounPhrases);
		}
		return nounPhrases;
	}

	public Map<String, List<String>> getEntities(String sentence) throws IOException {
		Map<String, List<String>> map = new HashMap<>();
		List<String> personList = new ArrayList<>();
		List<String> locationList = new ArrayList<>();
		List<String> organizationList = new ArrayList<>();

		String[] sentenceArr = sentence.split("\\W+");

		Span nameSpansP[] = nameFinderP.find(sentenceArr);
		personList = Arrays.asList(Span.spansToStrings(nameSpansP, sentenceArr));

		Span nameSpansL[] = nameFinderL.find(sentenceArr);
		locationList = Arrays.asList(Span.spansToStrings(nameSpansL, sentenceArr));

		Span nameSpansO[] = nameFinderO.find(sentenceArr);
		organizationList = Arrays.asList(Span.spansToStrings(nameSpansO, sentenceArr));

		map.put("person", personList);
		map.put("location", locationList);
		map.put("organization", organizationList);
		return map;

	}

	public NLPResult parseNLP(String sentence) {
		NLPResult result = new NLPResult();
		List<Entity> entities = new ArrayList<>();
		if (!StringUtils.isBlank(sentence)) {
			try {
				result.setMetaNouns(getNouns(sentence));
				Map<String, List<String>> namedEntityMap = getEntities(sentence);
				for (String key : namedEntityMap.keySet()) {
					Entity entity = result.new Entity();
					entity.setType(key.toUpperCase());
					entity.setValue(namedEntityMap.get(key));
					entities.add(entity);
				}
			} catch (Exception e) {
				log.error(e);
			}
		}
		result.setEntities(entities);
		return result;
	}

	public static void main(String[] args) throws Exception {
		NLPParser nlpParser = NLPParser.getInstance();
		System.out.println(nlpParser.parseNLP(" Gurgaon is a good city"));
		/*Charset charset = Charset.forName("UTF-8");
		ObjectStream<String> lineStream = new PlainTextByLineStream(new FileInputStream(""), charset);
		ObjectStream<NameSample> sampleStream = new NameSampleDataStream(lineStream);

		TokenNameFinderModel model;

		try {
		  model = NameFinderME.train("en", "person", sampleStream, TrainingParameters.defaultParams(),
		            TokenNameFinderFactory nameFinderFactory);
		}
		finally {
		  sampleStream.close();
		}

		try {
		  modelOut = new BufferedOutputStream(new FileOutputStream(modelFile));
		  model.serialize(modelOut);
		} finally {
		  if (modelOut != null) 
		     modelOut.close();      
		}*/
		
		
	}

}
