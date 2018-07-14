
package com.db.nlp.sentiment;

import java.util.ArrayList;
import java.util.Properties;

import com.db.common.utils.DBConfig;

import edu.stanford.nlp.ling.CoreAnnotations;
import edu.stanford.nlp.neural.rnn.RNNCoreAnnotations;
import edu.stanford.nlp.pipeline.Annotation;
import edu.stanford.nlp.pipeline.StanfordCoreNLP;
import edu.stanford.nlp.sentiment.SentimentCoreAnnotations.SentimentAnnotatedTree;
import edu.stanford.nlp.tagger.maxent.MaxentTagger;
import edu.stanford.nlp.trees.Tree;
import edu.stanford.nlp.util.CoreMap;

public class SentimentAnalyser {
	static StanfordCoreNLP pipeline=initializePipeline();
	
	static StanfordCoreNLP initializePipeline(){
		if(Boolean.parseBoolean(DBConfig.getInstance().getProperty("nlp.required"))){

		Properties props = new Properties();
		props.setProperty("annotators", "tokenize, ssplit, parse, sentiment");
			return new StanfordCoreNLP(props);
		}
		return null;
		
	}

	public int findSentiment(String text) {
		int mainSentiment = 2;
		if (pipeline!=null&& text != null && text.length() > 0) {
			int longest = 0;
			Annotation annotation = pipeline.process(text);
			for (CoreMap sentence : annotation.get(CoreAnnotations.SentencesAnnotation.class)) {
				Tree tree = sentence.get(SentimentAnnotatedTree.class);
				int sentiment = RNNCoreAnnotations.getPredictedClass(tree);
				String partText = sentence.toString();
				if (partText.length() > longest) {
					mainSentiment = sentiment;
					longest = partText.length();
				}
			}
		}
		return mainSentiment;
	}
	
	public static String findPOSTags(String text) {
		MaxentTagger tagger = new MaxentTagger("taggers/left3words-wsj-0-18.tagger");
		String tagged = tagger.tagString(text);
		return tagged;
	}

	public static void main(String[] args) {
		ArrayList<String> text = new ArrayList<String>();
		text.add("Nc");
		SentimentAnalyser sa = new SentimentAnalyser();
		//System.out.println(SentimentAnalyser.findPOSTags("neither good nor bad"));
		
		for(String sentence : text) {
			System.out.println(sa.findSentiment(sentence));
		}
	}
}
