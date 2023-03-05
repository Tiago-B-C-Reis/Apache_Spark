# Natural Language Processing Project

## Spam detection filter

The data is saved as smsspamcollection.csv. Here are the fields and their definitions:

    _c0 : Renamed to "class" - It's the label, reveling if the text it's spam or not spam.  
    _c1: Renamed to "text" - It's the text that the e-mail contains.

This project utilizes NPL methods to tokenize the 'text' data into features.
Then it's used different logistic models to predict if the e-mail was spam or not, like:
+ NaiveBayes.
+ Gradient-boosted tree classifier.
+ Random forest classifier.
+ Binomial logistic regression.
Then evaluate with:
+ 'MulticlassClassificationEvaluator' to find the accuracy of each model.
+ 'BinaryClassificationEvaluator' to find the AUC also for each model.
