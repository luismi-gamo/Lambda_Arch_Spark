from pyspark.mllib.regression import LabeledPoint, LinearRegressionWithSGD, LinearRegressionModel
from pyspark import SparkContext
import os
import datetime

sc = SparkContext(appName="ML_tests")

# Load and parse the data
def parsePoint(line):
    values = [float(x) for x in line.replace(',', ' ').split(' ')]
    return LabeledPoint(values[0], values[1:])

data = sc.textFile("./regression_example.data")
parsedData = data.map(parsePoint)
print parsedData.take(3)


# Build the model
model = LinearRegressionWithSGD.train(parsedData)

# Evaluate the model on training data
valuesAndPreds = parsedData.map(lambda p: (p.label, model.predict(p.features)))
print 'valuesAndPreds'
print valuesAndPreds.collect()

MSE = valuesAndPreds.map(lambda (v, p): (v - p)**2).reduce(lambda x, y: x + y) / valuesAndPreds.count()
print("Mean Squared Error = " + str(MSE))
# Save and load model
model_prefix = "./Regression_Models/myModelPath" + datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
model.save(sc, model_prefix)
#sameModel = LinearRegressionModel.load(sc, model_prefix)