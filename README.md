Frequent Itemset Analysis on Amazon Metadata

Firstly the sampled dataset was preprocessed using the preprocessing.py file to be able to be used for further processing.
Then a Kafka Producer was set up to stream the data which is supposed to stream the data meant to be consumed by the consumers.
Three Consumers Are Made
Consumer 1 : In this consumer the Apriori Algorithm is Implemented.
Consumer 2 : In this consumer the PCY Algorithm is Implemented.
Consumer 3 : In this consumer various other insights on the data are obtained and displayed.

Afterwards each consumer was modified to store the data in a database.
The bash file is used to automate this entire process so that commands dont have to be executed manually.  
