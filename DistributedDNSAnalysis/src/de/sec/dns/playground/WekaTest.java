package de.sec.dns.playground;

import java.io.File;
import java.io.FileReader;

import weka.classifiers.Evaluation;
import weka.classifiers.bayes.NaiveBayesMultinomial;
import weka.core.Instance;
import weka.core.Instances;

public class WekaTest {
	public static void main(String[] args) {
		try {
			FileReader reader = new FileReader(new File(
					"../../../../arff/2010-03-02.arff"));
			Instances trainingInstances = new Instances(reader);
			trainingInstances.setClass(trainingInstances.attribute("class"));

			System.out.println("loaded training with " + trainingInstances.numInstances()
					+ " instances with " + trainingInstances.numClasses()
					+ " classes and " + trainingInstances.numAttributes()
					+ " attributes.");
			
			reader = new FileReader(new File("../../../../arff/2010-03-03.arff"));			
			Instances testInstances = new Instances(reader);
			testInstances.setClass(testInstances.attribute("class"));

			System.out.println("loaded test with " + testInstances.numInstances()
					+ " instances with " + testInstances.numClasses()
					+ " classes and " + testInstances.numAttributes()
					+ " attributes.");
			
			NaiveBayesMultinomial bayes = new NaiveBayesMultinomial();
			bayes.buildClassifier(trainingInstances);			
			Evaluation eval = new Evaluation(trainingInstances);
			eval.evaluateModel(bayes, testInstances);
			
			System.out.println(eval.toMatrixString());
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
}
